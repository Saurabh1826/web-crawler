# TODO: Look into whether we need to do anything due to the program being multithreaded (eg: will there be race conditions, inconsistencies due to multiple threads accessing the same object (like the LRU caches), etc)

import time 
import requests 
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor
import os 
import hashlib 
import redis 
from datetime import datetime 
from collections import OrderedDict
import threading 

MOUNT_PATH = '/shared'
REDIS_HOST = ''
REDIS_PORT = 6379
MAX_PARSE_QUEUE_LEN = 300
DEFAULT_CRAWL_DELAY = 5
MAX_FETCH_WORKER_THREADS = 100
REQUESTS_TIMEOUT = 2

redisClient = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
redisOffset = '0-0'

# hostFetchTimes = {}

class BloomFilter:
    def __init__(self, size, hash_count):
        self.size = size 
        self.hash_count = hash_count 
        self.bit_array = [0] * size 

    def _hash(self, item, seed):
        return int(hashlib.md5((str(seed) + item).encode('utf-8')).hexdigest(), 16) % self.size

    def add(self, item):
        for i in range(self.hash_count):
            hash_value = self._hash(item, i)
            self.bit_array[hash_value] = 1

    def contains(self, item):
        for i in range(self.hash_count):
            hash_value = self._hash(item, i)
            if self.bit_array[hash_value] == 0:
                return False  # The item is definitely not in the set
        return True  # The item might be in the set (could be a false positive)

bloomFilter = BloomFilter(10000, 1000)

class LRUCache: 
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()
    
    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None 

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)
        
        self.cache[key] = value

robotsTxt = LRUCache(1000)
hostFetchTimes = LRUCache(10000)

def getUrls(): 

    # urls = []
    # try: 
    #     response = requests.get(FRONTIER_GET_PATH)
    #     if (response.status_code == 200): 
    #         try:
    #             data = response.json()
    #             urls = data['urls'].split(',')
    #         except requests.exceptions.JSONDecodeError:
    #             pass 
    # except requests.RequestException as e:
    #     pass

    # return urls 

    global redisOffset

    urls = set()

    messages = redisClient.xread({'urls': redisOffset}, count=100, block=1)
    for streamName, messageList in messages: 
        if (not streamName == 'urls'): 
            continue 
        for messageId, messageData in messageList: 
            urls.add(messageData['url'])
            redisOffset = messageId
    
    return list(urls)

def fetchRobots(url): 

    try: 
        parsedUrl = urlparse(url)
        robotsUrl = urlunparse((
            parsedUrl.scheme, 
            parsedUrl.hostname, 
            'robots.txt', 
            '',
            '',
            ''
        ))

        response = requests.get(robotsUrl, headers={'User-Agent': 'crawler'}, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status() 
        text = response.text 

        rules = {}
        user_agent = None

        for line in text.splitlines():
            line = line.strip()

            if not line or line.startswith('#'):
                continue

            if line.lower().startswith('user-agent:'):
                user_agent = line.split(':', 1)[1].strip().lower()
                if user_agent not in rules:
                    rules[user_agent] = {'disallow': [], 'allow': []}

            elif line.lower().startswith('disallow:'):
                if user_agent:
                    path = line.split(':', 1)[1].strip()
                    rules[user_agent]['disallow'].append(path)

            elif line.lower().startswith('allow:'):
                if user_agent:
                    path = line.split(':', 1)[1].strip()
                    rules[user_agent]['allow'].append(path)

            elif line.lower().startswith('crawl-delay:'):
                if user_agent:
                    delay = line.split(':', 1)[1].strip()
                    try:
                        rules[user_agent]['crawl-delay'] = float(delay)
                    except ValueError:
                        rules[user_agent]['crawl-delay'] = DEFAULT_CRAWL_DELAY 

        robotsTxt.put(parsedUrl.hostname, rules)
    except Exception as e: 
        pass 

def canCrawl(rules, user_agent, path):
    user_agent = user_agent.lower()
    
    if user_agent in rules:
        disallowed_paths = rules[user_agent]['disallow']
        allowed_paths = rules[user_agent]['allow']

        for p in allowed_paths:
            if path.startswith(p):
                return True  

        for p in disallowed_paths:
            if path.startswith(p):
                return False 

    return True

def fetchUrl(url): 

    # print(f'fetchUrl {url}')

    try:

        # First check if we have already visited this url
        check = bloomFilter.contains(url)
        if (check): # If bloom filter check is true, we need to confirm that this url is actually in the visited set 
            if (redisClient.sismember('visitedUrls', url)): # Url has already been visited, so we skip it 
                # print('Visited')
                return None, None  
        
        # print(f'Finished checking if {url} was visited')

        parsedUrl = urlparse(url)
        hostName = parsedUrl.hostname

        # Get robots.txt if necessary 
        rules = robotsTxt.get(hostName)
        if (rules is None): 
            fetchRobots(url)
            rules = robotsTxt.get(hostName)
        if (rules is None): # There was an error fetching robots.txt -- we were unable to get the robots.txt file
            # print(f'Error fetching {url} robots.txt')
            return url, None 
        
        # print(f'Finished getting {url} robots.txt')

        # Check that the path is allowed to be crawled by robots.txt (if not, return None, None)
        if (not canCrawl(rules, '*', parsedUrl.path)): # Url is not allowed to be crawled -- mark the url as handled (so we don't try to crawl it again)
            bloomFilter.add(url)
            redisClient.sadd('visitedUrls', url)
            # print('Not allowed')
            return None, None 
        
        # print(f'Finished checking if {url} can be crawled')

        # if (hostName in hostFetchTimes): 
        #     crawlDelay = DEFAULT_CRAWL_DELAY
        #     if ('*' in rules and 'crawl-delay' in rules['*']): 
        #         crawlDelay = rules['*']['crawl-delay']
        #     currTime = datetime.now()
        #     diffTime = (currTime-hostFetchTimes[hostName]).total_seconds()
        #     if (crawlDelay is not None and diffTime < crawlDelay): # Not enough time has elapsed for the domain, we will re-enqueue the url 
        #         return url, None 
        #     hostFetchTimes[hostName] = datetime.now()
        # else: 
        #     hostFetchTimes[hostName] = datetime.now()
        if (hostFetchTimes.get(hostName) is not None): 
            crawlDelay = DEFAULT_CRAWL_DELAY
            if ('*' in rules and 'crawl-delay' in rules['*']): 
                crawlDelay = rules['*']['crawl-delay']
            currTime = datetime.now()
            diffTime = (currTime-hostFetchTimes.get(hostName)).total_seconds()
            if (crawlDelay is not None and diffTime < crawlDelay): # Not enough time has elapsed for the domain, we will re-enqueue the url 
                # print(f'Not enough time elapsed for host of {url}')
                return url, None 
            hostFetchTimes.put(hostName, datetime.now())
        else: 
            hostFetchTimes.put(hostName, datetime.now())
        
        # print(f'Fetching {url}')

        response = requests.get(url, headers={'User-Agent': 'crawler'}, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        if (response.status_code == 200): 
            data = response.text 
            # print(f'Finished fetching {url}')
            
            return url, data 
    except Exception as e:
        # print(f'fetchUrl for {url} had an exception: {e}')
        pass 

    # print(f'fetchUrl for {url} had an exception')

    return url, None

def sendUrlsToRedis(urls): 
    for url in urls: 
        redisClient.xadd('urls', {'url': url})
    
    print(f'Reenqueued {len(urls)} urls')




while (True): 

    # Check if we should get new urls from the frontier. If we already have too many pages in the queue to be parsed, don't get new urls
    parseQueueLen = len(os.listdir(MOUNT_PATH))
    if (parseQueueLen >= MAX_PARSE_QUEUE_LEN): 
        urls = None 
    else: 
        urls = getUrls()

    # If no new urls were received, sleep for some time before checking with the frontier again 
    if (not urls): 
        print('No new urls')
        time.sleep(0.5)
        continue 

    print(f'Got {len(urls)} urls from redis')

    # Get html content for all urls in a manner that is robust to failures, redirects, etc. Write the results to disk at the shared volume 
    # mount path. The files should have their first line be the url of the hostname (scheme+protocol+urlparse().hostname) followed by \n. The 
    # rest of the file is the html content. The file name will be the hash of the file content. Make the url requests in parallel 
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKER_THREADS) as executor:
        results = list(executor.map(fetchUrl, urls))
    
    if (not results): 
        continue 

    print('Finished fetching pages')

    reenqueueUrls = set()
    numPagesProcessed = 0

    for result in results: 
        url, htmlContent = result 
        if (url and htmlContent): 
            numPagesProcessed += 1
            parsedUrl = urlparse(url)
            scheme = parsedUrl.scheme
            hostname = parsedUrl.hostname
            hostUrl = urlunparse((scheme, hostname, '', '', '', ''))
            fileContent = hostUrl+'\n'+htmlContent
            fileName = hashlib.sha256(fileContent.encode('utf-8')).hexdigest()+'.txt'
            filePath = os.path.join(MOUNT_PATH, fileName)
            with open(filePath, 'w') as file:
                file.write(fileContent)
            bloomFilter.add(url)
            redisClient.sadd('visitedUrls', url)
        elif (url): # Either an error occured when fetching the url or not enough time elapsed for the host, so we need to re-enqueue the url (if the url is not allowed to be crawled, fetchUrl would have returned None for the url)
            reenqueueUrls.add(url)
    
    print(f'Finished processing {numPagesProcessed} pages')
    
    # Reenqueue the necessary urls 
    threading.Thread(target=sendUrlsToRedis, args=(reenqueueUrls,)).start()



