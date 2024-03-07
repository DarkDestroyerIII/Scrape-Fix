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
    links = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        full_url = normalize_url(full_url)
        if full_url.startswith(base_url):
            links.add(full_url)
    return links


counter_lock = asyncio.Lock()
shared_counter = {'count': 0}
async def scrape_site(queue, base_url, emails_found, emails_found_lock, visited_urls, max_depth, session, semaphore):
    global alogging
    global shared_counter
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

        semaphore = asyncio.Semaphore(10)  # Adjust based on your rate limit needs

        tasks = []
        for _ in range(6):  # Adjust number of workers as needed
            task = asyncio.create_task(scrape_site(queue, base_url, emails_found, emails_found_lock, visited_urls, max_depth, session, semaphore))
            tasks.append(task)
        await queue.join()
        for _ in range(len(tasks)):  # Ensure one sentinel for each worker
         await queue.put("END")
          
        await asyncio.gather(*tasks)

        with open('emails_found.txt', 'w') as file:
            for email, page in emails_found.items():
                file.write(f"Found on: {page}\n{email}\n\n")

if __name__ == "__main__":
  urls ={"https://www.kashlegal.com","https://www.arashlaw.com","https://www.eyllaw.com"}
  #for i in urls:
  asyncio.run(main("https://www.kashlegal.com", 5))
