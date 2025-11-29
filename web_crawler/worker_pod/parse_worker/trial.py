from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import threading 
import requests 
import hashlib 

with open('/Users/saurabhmallela/web_crawler/worker_pod/shared/sldjs.txt', 'r') as file:
    html_content = file.read()

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

def sendUrlsToFrontier(data): 
    try: 
        requests.post('http://ec2-54-234-57-11.compute-1.amazonaws.com:8000', json=data)
    except requests.RequestException as e:
        pass

urls = extractAndNormalizeUrls('https://en.wikipedia.org/', html_content)
data = {"urls": ",".join(urls)}

# threading.Thread(target=sendUrlsToFrontier(data)).start()
# requests.post('http://ec2-54-234-57-11.compute-1.amazonaws.com:8000', json=data)

response = requests.get('https://en.wikipedia.org/wiki/Abstract_algebra', headers={'User-Agent': 'crawler'})
print(response)
