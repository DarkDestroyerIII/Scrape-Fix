import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import threading
from queue import Queue
import time

import logging

logging.basicConfig(level=logging.DEBUG,
  format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
  filename='web_scraping.log',
  filemode='w')


console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)



def normalize_url(url):
    if "://" in url:
        parts = url.split("://", 1)
        normalized = parts[0] + "://" + parts[1].replace("//", "/")
    else:
        normalized = "http://" + url.replace("//", "/")
    return normalized

def find_emails(soup, url, emails_found):
    emails_found_lock = threading.Lock()
    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    emails = email_pattern.findall(str(soup))
    for email in set(emails):
      with emails_found_lock:
        emails_found[email] = url

def find_links(soup, base_url):
    links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        full_url = normalize_url(full_url)
        if full_url.startswith(base_url):
            links.append(full_url)
    return links

processed_urls_count = 0
count_lock = threading.Lock()

def scrape_site(queue, base_url, emails_found, visited_urls, max_depth):
    global processed_urls_count
    while True:
        item = queue.get()
        if item is None:  # Sentinel value to signal end of work
            logging.debug('Received sentinel, exiting')
            break
        url, depth = item
        logging.debug(f'Starting scrape of {url} at depth {depth}')
        if url in visited_urls or depth > max_depth:
            logging.debug(f'Finished scrape of {url}')
            queue.task_done()
            continue

        # print(f"Scraping {url} at depth {depth}")

        visited_urls.add(url)

        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            response = requests.get(url, headers=headers)
            #time.sleep(0.1)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                find_emails(soup, url, emails_found)
                if depth < max_depth:
                    links = find_links(soup, base_url)
                    # logging.info("Found %d links from %s", len(links), url)
                    for link in links:
                        if link not in visited_urls:
                          with count_lock:
                            processed_urls_count += 1
                            if processed_urls_count % 100 == 0:  # Adjust the modulo value as needed
                              logging.info(f"Processed {processed_urls_count} URLs so far.")
                            queue.put((link, depth + 1))
        except Exception as e:
            print(f"Error scraping {url}: {e}")
        finally:
            queue.task_done()

def main(start_url, max_depth):
    parsed_url = urlparse(start_url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    emails_found = {}
    visited_urls = set()

    queue = Queue()
    queue.put((start_url, 0))

    threads = [threading.Thread(target=scrape_site, args=(queue, base_url, emails_found, visited_urls, max_depth)) for _ in range(2)]

    for thread in threads:
        thread.start()

    queue.join()  # Block until all tasks are done

    # Signal all threads to exit
    for _ in range(len(threads)):
        queue.put(None)
    for thread in threads:
        thread.join()

    with open('emails_found.txt', 'a') as file:  # Use 'w' to overwrite or 'a' to append
      for email, page in emails_found.items():
        file.write(f"Found on: {page}\n{email}\n\n")

if __name__ == "__main__":
  urls = {"https://www.kashlegal.com","https://www.arashlaw.com","https://www.eyllaw.com"}
  #for i in urls:
  main("https://www.arashlaw.com", 5)
