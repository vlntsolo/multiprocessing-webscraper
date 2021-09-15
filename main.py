import asyncio
import logging
from multiprocessing import Queue, Process, cpu_count
import queue
from threading import Thread
import time
import aiohttp
import aiosqlite
from bs4 import BeautifulSoup

PROCESSES = cpu_count() #P parameter
NUM_THREADS = 10 #T parameter

TARGET_RESOURCES = [ 
    'https://example.org/',
    'https://www.python.org/',
    'https://www.djangoproject.com/',
    'https://www.lipsum.com/',
]

async def parse(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.title.string
            links = []
            for link in soup.find_all('a'):
                links.append(link.get('href'))
            links_str = ', '.join(links)
            obj = {
                'title': str(title),
                'links': links_str,
            }
            return obj

def add_tasks(task_queue):
    for url in TARGET_RESOURCES:
        task_queue.put(url)
    return task_queue


async def write_to_sqlite(record: dict):

    title = record.get('title')
    links = record.get('links')

    if title is not None:
        async with aiosqlite.connect('crawler.db') as db:
            await db.execute('''CREATE TABLE IF NOT EXISTS pages (title, links)''')
            await db.execute(f"INSERT INTO pages VALUES ('{title}','{links}')")
            await db.commit()


class TaskConsumer:
    '''
    Consumer class to handle flow to asyncio
    for multiprocessing
    '''
    def __init__(self):
        pass

    async def thread_worker(self, q):
        while True:
            task = q.get()
            await write_to_sqlite(task)
            q.task_done()

    async def process_tasks_from_queue(self, task_queue, results_queue):
        while not task_queue.empty():
            url = task_queue.get()
            result = await parse(url)
            results_queue.put(result)

    def process(self, task_queue, results_queue):
        #Asyncio coroutine
        asyncio.run(self.process_tasks_from_queue(task_queue, results_queue))

    def thread(self, thread_queue):
        #Asyncio coroutine
        asyncio.run(self.thread_worker(thread_queue))


def run():
    #New multiprocess queue
    # pipe_list = []
    start = time.time()
    empty_task_queue = Queue()
    full_task_queue = add_tasks(empty_task_queue)
    processes = []
    results_queue = Queue()
    thread_queue = queue.Queue()

    print(f"Running with {PROCESSES} processes (scraper) and with {NUM_THREADS} threads for sqlite")

    #Spawning processes to scrape webpage
    for n in range(PROCESSES):
        p = Process(target=TaskConsumer().process, args=(full_task_queue, results_queue, ))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    
    #Dump results for threads consumers
    print("results_queue", results_queue.qsize())
    while not results_queue.empty():
        thread_queue.put(results_queue.get())

    print("results_queue", results_queue.qsize())
    print("thread_queue", thread_queue.qsize())

    for i in range(NUM_THREADS):
        worker = Thread(target=TaskConsumer().thread, args=(thread_queue, ), daemon=True)
        worker.start()
    #Block thread queue until all items are processed
    thread_queue.join()

    print(f"Time taken = {time.time() - start:.10f}")


if __name__ == "__main__":
    run()
