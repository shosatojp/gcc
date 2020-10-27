from asyncio.futures import Future
import collections
from concurrent.futures import ProcessPoolExecutor
from typing import Deque, Dict
import asyncio
import os


class Collector():
    def __init__(self) -> None:
        self._futures = set()
        self._pool = ProcessPoolExecutor(os.cpu_count())
        self._queues: Dict[str, asyncio.Queue] = {}

    async def add_future(self, tag, coro):
        if tag not in self._queues:
            self._queues[tag] = asyncio.Queue(20)

        async def a():
            await coro
            self._queues[tag].get_nowait()
        await self._queues[tag].put(None)
        self._futures.add(asyncio.ensure_future(a()))

    async def run_in_executor(self, fn, *args):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._pool, fn, *args)

    async def run(self, coro):
        await self.add_future('run', coro)
        await self.purge_finished_futures()

    async def purge_finished_futures(self):
        while True:
            if len(self._futures) == 0:
                return
            else:
                self._futures = {e for e in self._futures if not e.done()}
            await asyncio.sleep(1)

    async def queued_paging(self, pagestart, pageend, mkcorofn,
                            queue_size=2):

        pages = []
        q = UnboundQueue(queue_size)

        for page_num in range(pagestart, pageend+1):
            async def worker(p):
                r = await mkcorofn(p)
                q.decrement()
                if r == False:
                    q.end()

            if await q.increment() == False:
                break
            pages.append(asyncio.create_task(worker(page_num)))

        await asyncio.gather(*pages)


class UnboundQueue():
    def __init__(self, max_size) -> None:
        self.max_size = max_size
        self.count = 0
        self.incrementers: Deque[Future] = collections.deque()
        self.finished = False

    def wake_next(self):
        for e in self.incrementers:
            if not e.done():
                e.set_result(None)
                break

    async def increment(self):
        while self.count >= self.max_size:
            incrementer = asyncio.get_event_loop().create_future()
            self.incrementers.append(incrementer)
            await incrementer
            if self.finished:
                return False

        self.count += 1
        return True

    def decrement(self):
        self.count -= 1
        self.wake_next()

    def end(self):
        self.finished = True
        self.wake_next()
