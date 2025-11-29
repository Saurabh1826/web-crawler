# TODO: Look into whether we need to do anything due to the program being multithreaded (eg: will there be race conditions, inconsistencies due to multiple threads accessing the same object, etc)

import time 
import os 
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import threading 
# import requests 
import redis 
import boto3
import hashlib 

BATCH_SIZE = 10
MOUNT_PATH = '/shared'
REDIS_HOST = ''
REDIS_PORT = 6379
BUCKET_NAME = ''

redisClient = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
s3 = boto3.client('s3')


def normalizeUrl(hostUrl, url):
    parsedUrl = urlparse(url)

    if (not parsedUrl.scheme and not parsedUrl.netloc): 
        parsedUrl = urlparse(urljoin(hostUrl, url))

    if (not parsedUrl.scheme or not parsedUrl.netloc):
        return ''

    scheme = parsedUrl.scheme.lower()
    netloc = parsedUrl.netloc.lower()
    path = parsedUrl.path.lower()

    if (scheme == 'http' and parsedUrl.port == 80) or (scheme == 'https' and parsedUrl.port == 443):
        netloc = parsedUrl.hostname

    path = path.rstrip('/')

    normalizedUrl = urlunparse((
        scheme, 
        netloc, 
        path, 
        '',
        '',
        ''
    ))

    return normalizedUrl

def extractAndNormalizeUrls(hostUrl, page): 
    soup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a', href=True)

    urls = set()
    for link in links: 
        normalizedUrl = normalizeUrl(hostUrl, link['href'])
        if (not normalizedUrl == ''): 
            urls.add(normalizedUrl)

    return urls 

# def sendUrlsToFrontier(data): 
#     try: 
#         requests.post(FRONTIER_POST_PATH, json=data)
#     except requests.RequestException as e:
#         pass

def sendUrlsToRedis(urls): 
    for url in urls: 
        redisClient.xadd('urls', {'url': url})
    
    print(f'Sent {len(urls)} urls to redis')

def sendPagesToStorage(pages): 
    for page in pages: 
        try: 
            s3.put_object(Bucket=BUCKET_NAME, Key=hashlib.sha256(page.encode('utf-8')).hexdigest(), Body=page)
        except Exception as e: 
            print(e)
            pass

    print(f'Sent {len(pages)} pages to storage')

while (True): 

    # Get up to BATCH_SIZE pages from MOUNT_PATH 
    print('Fetching new batch from disk')
    pages = []
    for entry in os.listdir(MOUNT_PATH):
        fullPath = os.path.join(MOUNT_PATH, entry)
        if os.path.isfile(fullPath):
            with open(fullPath, 'r') as file:
                hostUrl = file.readline().strip('\n')
                page = file.read()
            pages.append((hostUrl, page))
        
            # Delete the file after reading it 
            try:
                os.remove(fullPath)
            except OSError as e:
                pass
        
        if (len(pages) == BATCH_SIZE): 
            break 
    
    print(f'Got {len(pages)} pages from disk')
    
    # If there are no pages to process, wait for some time before checking again 
    if (len(pages) == 0): 
        # print ('No new pages to process')
        time.sleep(0.2)
        continue

    pagesToStore = []
    urls = set()
    # Extract normalized urls from all obtained pages
    for hostUrl, page in pages: 
        urls.update(extractAndNormalizeUrls(hostUrl, page))
        
        # Trim page down to smaller size and attach the hostUrl to send to storage
        pagesToStore.append(hostUrl+'\n'+page[:1000])
    
    print('Finished extracting urls')

    # Send urls to the frontier 
    # data = {"urls": ",".join(urls)}
    # threading.Thread(target=sendUrlsToFrontier(data)).start()
    threading.Thread(target=sendUrlsToRedis, args=(urls,)).start()

    # Send pages to storage
    threading.Thread(target=sendPagesToStorage, args=(pagesToStore,)).start()



