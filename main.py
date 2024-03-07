import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
from aiologger import Logger
from aiologger.handlers.files import AsyncFileHandler
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
import sys

import psutil

from lxml import html

async def setup_logger():
    logger = Logger(level=LogLevel.DEBUG)
    file_formatter = Formatter('%(asctime)s %(levelname)s: %(message)s')
    file_handler = AsyncFileHandler(filename='web_scraping.log', mode='w')
    file_handler.level = LogLevel.DEBUG
    file_handler.formatter = file_formatter
    logger.add_handler(file_handler)

    console_formatter = Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console_handler = AsyncStreamHandler(stream=sys.stdout)
    console_handler.level = LogLevel.INFO
    console_handler.formatter = console_formatter
    logger.add_handler(console_handler)

    return logger
alogging = None  # Will be set in main()


target_concurrency = 10
async def adjust_concurrency_based_on_resources(cpu_threshold=90, memory_threshold=500*1024*1024, minimum_concurrency=5, maximum_concurrency=20, check_interval=10):
  """
  Dynamically adjusts the level of concurrency based on system resource usage (CPU and memory).

  Parameters:
  - cpu_threshold: CPU usage percentage that triggers a concurrency adjustment.
  - memory_threshold: Memory usage threshold (in bytes) that triggers a concurrency adjustment.
  - minimum_concurrency: The minimum number of concurrent tasks allowed.
  - maximum_concurrency: The maximum number of concurrent tasks allowed.
  - check_interval: How frequently (in seconds) to check system resource usage.
  """
  global target_concurrency  # Reference the global variable controlling target concurrency level

  while True:
      await asyncio.sleep(check_interval)  # Pause execution for the specified interval to wait before re-checking resources

      # Gather current system resource usage metrics
      cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage percentage over the last second
      memory_usage = psutil.virtual_memory().used  # Current memory usage in bytes

      # Check if resource usage exceeds thresholds and adjust concurrency accordingly
      if (cpu_usage > cpu_threshold or memory_usage > memory_threshold) and target_concurrency > minimum_concurrency:
          target_concurrency -= 1  # Decrease target concurrency level if above minimum
          if alogging is not None:  # Log the adjustment if logging is set up
              await alogging.info(f"Reduced target concurrency to {target_concurrency}. CPU: {cpu_usage}%, Memory: {memory_usage/(1024*1024)} MB")
      elif cpu_usage < cpu_threshold and memory_usage < memory_threshold and target_concurrency < maximum_concurrency:
          target_concurrency += 1  # Increase target concurrency level if below maximum and resources are under thresholds
          if alogging is not None:  # Log the adjustment
              await alogging.info(f"Increased target concurrency to {target_concurrency}. CPU: {cpu_usage}%, Memory: {memory_usage/(1024*1024)} MB")



def normalize_url(url):
    if "://" in url:
        parts = url.split("://", 1)
        normalized = parts[0] + "://" + parts[1].replace("//", "/")
    else:
        normalized = "http://" + url.replace("//", "/")
    return normalized

async def find_emails(soup, url, emails_found, emails_found_lock):
    email_pattern = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    emails = set(email_pattern.findall(str(soup)))
    if emails:
        async with emails_found_lock:
            for email in emails:
                emails_found[email] = url

def find_links(soup, base_url):
    tree = html.fromstring(str(soup))
    links = set()
    for element in tree.xpath('//a/@href'):
      full_url = urljoin(base_url, element)
      full_url = normalize_url(full_url)
      if full_url.startswith(base_url):
        links.add(full_url)
    return links


counter_lock = asyncio.Lock()
shared_counter = {'count': 0}
async def scrape_site(queue, base_url, emails_found, emails_found_lock, visited_urls, max_depth, session, semaphore):
    global alogging
    global shared_counter

    exclude = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 'jpg' , 'jpeg', 'png', 'gif', 'bmp', 'mp4', 'avi', 'mp3','.zipx', '.zip', '.rar', '.7z', '.tar', '.gz', '.iso', '.cab',)
  
    while True:
        item = await queue.get()
        if item == "END":  # Sentinel value to signal end of work
            await queue.put("END")  # Propagate the sentinel to other workers
          
            if alogging is not None:
              await alogging.debug('Received sentinel, exiting')
              
            queue.task_done()
            break

        url, depth = item
        if url in visited_urls or depth > max_depth:
            queue.task_done()
            continue
        if alogging is not None:
          await alogging.debug(f'Starting scrape of {url} at depth {depth}')
        visited_urls.add(url)
        async with semaphore:  # Rate limiting
            try:
                async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'}) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'lxml')
                        await find_emails(soup, url, emails_found, emails_found_lock)
                        if depth < max_depth:
                            links = find_links(soup, base_url)
                            for link in links:
                                if link not in visited_urls:
                                  if link.endswith(exclude):
                                    continue
                                  await queue.put((link, depth + 1))
                            async with counter_lock:
                              shared_counter['count'] += 1
                              if shared_counter['count'] % 25 == 0:
                                if alogging is not None:
                                  await alogging.info(f"Processed {shared_counter['count']} URLs so far.")

            except Exception as e:
                if alogging:
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
    emails_found_lock = asyncio.Lock()
    visited_urls = set()
    queue = asyncio.Queue()
    await queue.put((start_url, 0))
    
    semaphore = asyncio.Semaphore(6)
    
    # Initial setup for dynamic adjustment task
    adjust_task = asyncio.create_task(adjust_concurrency_based_on_resources())

    tasks = []  # List to keep track of active tasks
    while not queue.empty() or any(not task.done() for task in tasks):
        # Launch new tasks if below target concurrency
        while len(tasks) < target_concurrency and not queue.empty():
            task = asyncio.create_task(scrape_site(queue, base_url, emails_found, emails_found_lock, visited_urls, max_depth, session, semaphore))
            tasks.append(task)

        # Clean up completed tasks
        tasks = [task for task in tasks if not task.done()]

        await asyncio.sleep(1)  # Short sleep to prevent tight loop

    # Clean up
    adjust_task.cancel()  # Cancel the dynamic adjustment task
    await asyncio.gather(*tasks, return_exceptions=True)  # Wait for all tasks to complete

    with open('emails_found.txt', 'w') as file:
        for email, page in emails_found.items():
            file.write(f"Found on: {page}\n{email}\n\n")

if __name__ == "__main__":
  urls ={"https://www.kashlegal.com","https://www.arashlaw.com","https://www.eyllaw.com"}
  #for i in urls:
  asyncio.run(main("https://www.kashlegal.com", 5))
