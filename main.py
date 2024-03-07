import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
#import threading
from queue import Queue
import time

import aiohttp

import logging

import async_timeout
import asyncio
from aiologger import Logger
from aiologger.handlers.files import AsyncFileHandler
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
import sys



# logging.basicConfig(level=logging.DEBUG,
#   format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s',
#   filename='web_scraping.log',
#   filemode='w')


# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# console.setFormatter(formatter)
# logging.getLogger('').addHandler(console)


async def setup_logger():
  # Initialize a logger instance
  logger = Logger(level=LogLevel.DEBUG)

  # Set up file handler
  file_formatter = Formatter('%(asctime)s %(levelname)s: %(message)s')
  file_handler = AsyncFileHandler(filename='web_scraping.log', mode='w')
  file_handler.level = LogLevel.DEBUG
  file_handler.formatter = file_formatter
  logger.add_handler(file_handler)

  # Set up console handler
  console_formatter = Formatter('%(name)-12s: %(levelname)-8s %(message)s')
  console_handler = AsyncStreamHandler(stream=sys.stdout)
  console_handler.level = LogLevel.INFO  # Set console log level to INFO
  console_handler.formatter = console_formatter
  logger.add_handler(console_handler)

  return logger


#alogging = asyncio.run(setup_logger())#Making this global so that all functions can use it
alogging = None #global so all can use


def normalize_url(url):
    if "://" in url:
        parts = url.split("://", 1)
        normalized = parts[0] + "://" + parts[1].replace("//", "/")
    else:
        normalized = "http://" + url.replace("//", "/")
    return normalized

async def find_emails(soup, url, emails_found):
    emails_found_lock = asyncio.Lock()
    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    emails = email_pattern.findall(str(soup))
    for email in set(emails):
      async with emails_found_lock:
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
visited_urls_lock = asyncio.Lock()

async def scrape_site(queue, base_url, emails_found, visited_urls, max_depth, session):
  global alogging
  global visited_urls_lock
  global processed_urls_count
  while True:
      try:
          item = await queue.get()
      except asyncio.QueueEmpty:
          # No more items to process
          break

      if item is None:  # Sentinel value to signal end of work
        if alogging is not None:
          await alogging.debug('Received sentinel, exiting')
        break

      url, depth = item
      async with visited_urls_lock:
        if url in visited_urls or depth > max_depth:
            # if alogging is not None:
            #   await alogging.debug(f'Finished scrape of {url}')
            queue.task_done()
            continue
        if alogging is not None:
          await alogging.debug(f'Starting scrape of {url} at depth {depth}')
  
        visited_urls.add(url)
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            async with async_timeout.timeout(20):
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        await find_emails(soup, url, emails_found)
                        if depth < max_depth:
                            links = find_links(soup, base_url)
                            for link in links:
                                if link not in visited_urls:
                                    await queue.put((link, depth + 1))
                                    processed_urls_count += 1
                                    if processed_urls_count % 100 == 0:
                                      if alogging is not None:
                                        await alogging.info(f"Processed {processed_urls_count} URLs so far.")
        except Exception as e:
          if alogging is not None:
            await alogging.error(f"Error scraping {url}: {e}")
        finally:
            queue.task_done()
  
async def main(start_url, max_depth):
  global alogging
  alogging = await setup_logger()
  
  async with aiohttp.ClientSession() as session:
      parsed_url = urlparse(start_url)
      base_url = parsed_url.scheme + "://" + parsed_url.netloc
      emails_found = {}
      visited_urls = set()

      queue = asyncio.Queue()
      await queue.put((start_url, 0))

      tasks = []
      for _ in range(4):  # Adjust number of workers as needed
          task = asyncio.create_task(scrape_site(queue, base_url, emails_found, visited_urls, max_depth, session))
          tasks.append(task)

      await queue.join()  # Wait for queue to be processed

      # Cancel our worker tasks.
      for task in tasks:
          task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)

      with open('emails_found.txt', 'a') as file:
          for email, page in emails_found.items():
              file.write(f"Found on: {page}\n{email}\n\n")
            
if __name__ == "__main__":
  urls = {"https://www.kashlegal.com","https://www.arashlaw.com","https://www.eyllaw.com"}
  #for i in urls:
  asyncio.run(main("https://www.eyllaw.com", 5))
