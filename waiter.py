from collections import namedtuple
import random
from typing import Dict, List
import urllib.parse
import time
import asyncio
import os
import json


class LockTime():
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.time = time.time()

    def update_time(self, t=None):
        self.time = t or time.time()


class Waiter():
    def __init__(self, default_wait: List[str], waitlist_file: str = None):
        self._waitlist = {}
        if waitlist_file and os.path.exists(waitlist_file):
            with open(waitlist_file, 'rt', encoding='utf-8') as f:
                obj: dict = json.load(f)
                for key, item in obj.items():
                    wargs = list(map(lambda e: e.strip(), str(item).split(' ')))
                    self._waitlist[key] = select_waiter(wargs)

        if '*' not in self._waitlist:
            if default_wait:
                self._waitlist['*'] = select_waiter(default_wait)
            else:
                self._waitlist['*'] = DefaultWaiter()

    async def wait(self, url: str):
        parsed_url = urllib.parse.urlparse(url)
        host = parsed_url.hostname

        if host in self._waitlist:
            waiter: DefaultWaiter = self._waitlist[host]
        else:
            waiter: DefaultWaiter = self._waitlist['*']

        await waiter.wait(url)


class DefaultWaiter():
    def __init__(self):
        self._table: Dict[str, LockTime] = {}

    async def wait(self, url: str):
        await self._wait(url, 0)

    async def _wait(self, url: str, sec: int):
        host = urllib.parse.urlparse(url).hostname

        if host in self._table:
            await self._table[host].lock.acquire()
            now = time.time()
            if now > self._table[host].time + sec:
                self._table[host].update_time(now)
            else:
                ws = max(sec - (now - self._table[host].time), 0)
                await asyncio.sleep(ws)
                self._table[host].update_time()
            self._table[host].lock.release()
        else:
            self._table[host] = LockTime()


class ConstWaiter(DefaultWaiter):
    def __init__(self, sec: int):
        super(ConstWaiter, self).__init__()
        self.sec = sec

    async def wait(self, url: str):
        await self._wait(url, self.sec)


class RandomWaiter(DefaultWaiter):
    def __init__(self, min: int, max: int):
        super(RandomWaiter, self).__init__()
        self.min = min
        self.max = max

    async def wait(self, url: str):
        await self._wait(url, random.randrange(self.min, self.max))


def select_waiter(wargs: list = [1]):
    if (len(wargs) == 1):
        return ConstWaiter(float(wargs[0]))
    elif (len(wargs) == 2 and wargs[0] == 'const'):
        return ConstWaiter(float(wargs[1]))
    elif len(wargs) == 3 and wargs[0] == 'random':
        return RandomWaiter(float(wargs[1]), float(wargs[2]))
    else:
        print('invalied wait args')
        exit(1)
