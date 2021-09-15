import asyncio
import logging
import multiprocessing
import time

import aiohttp
import aiosqlite
from bs4 import BeautifulSoup

PROCESSES = multiprocessing.cpu_count() #P parameter

TARGET_RESOURCES = [ 
    'https://example.org/',
    'https://www.python.org/',
    'https://www.djangoproject.com/',
    'https://www.lipsum.com/',
]
results_queue = multiprocessing.Queue()

async def parse(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            # print("Status:", response.status)
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for link in soup.find_all('a'):
                links.append(link.get('href'))
            links_str = ', '.join(links)
            try:
                async with aiosqlite.connect('crawler.db') as db:
                    await db.execute('''CREATE TABLE IF NOT EXISTS pages (title, links)''')
                    await db.execute(f"INSERT INTO pages VALUES ('{soup.title.string}','{links_str}')")
                    await db.commit()
            except:
                logging.warning("SQLite writing error")


def add_tasks(task_queue):
    for url in TARGET_RESOURCES:
        task_queue.put(url)
    return task_queue

class TaskConsumer:
    '''
    Consumer class to instantiate regular method 
    for multiprocessing
    '''
    def __init__(self):
        pass

    async def process_tasks(self, task_queue):
        while not task_queue.empty():
            url = task_queue.get()
            await parse(url)
        return True

    def process(self, task_queue):
        #Asyncio coroutine
        asyncio.run(self.process_tasks(task_queue))


def run():
    empty_task_queue = multiprocessing.Queue()
    full_task_queue = add_tasks(empty_task_queue)
    processes = []
    print(f"Running with {PROCESSES} processes")
    start = time.time()
    for n in range(PROCESSES):
        p = multiprocessing.Process(target=TaskConsumer().process, args=(full_task_queue,))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print(f"Time taken = {time.time() - start:.10f}")



if __name__ == "__main__":
    run()
