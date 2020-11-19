import asyncio
from asyncio.locks import Semaphore
from typing import Any, List, Optional
import aiohttp
from waiter import Waiter
from reporter import Reporter
from cacher import Cacher
from collector import Collector
import bs4
import urllib.parse
import argparse
import signal
import os

SITE_ENCODING = 'utf-8'


def get_post_urls(html):
    doc = bs4.BeautifulSoup(html, 'lxml')
    return [
        e.select_one('a')['href']
        for e in doc.select('.hentry')
    ]


def get_pict_urls(html):
    doc = bs4.BeautifulSoup(html, 'lxml')
    l = []
    e: bs4.element.Tag
    for e in doc.select('div.tw_matome > a > img'):
        url = e.parent['href']
        l.append(url)
    return l


async def download_file(url, filename, cacher: Cacher, useragent=''):
    print(url)
    async with aiohttp.request('get', url, headers={
        'user-agent': useragent,
    }) as res:
        content = await res.read()
        cacher.set(filename, content)


class Anicobin(Collector):
    def __init__(self, reporter, waiter, outdir, useragent) -> None:
        super(Anicobin, self).__init__()
        self.reporter: Reporter = reporter
        self.waiter = waiter
        self.outdir = outdir
        self.useragent = useragent
        self.cacher = Cacher(self.outdir)
        self.semaphore = Semaphore(2)

    async def get(self, url):
        filename = urllib.parse.quote(url, safe='') + '.html'
        cache, _ = self.cacher.get(filename)
        if cache:
            html = cache
        else:
            await self.waiter.wait(url)
            print('fetching', url)
            async with aiohttp.request('get', url, headers={'user-agent': self.useragent}) as req:
                content = await req.read()
                html = content.decode(SITE_ENCODING)
                self.cacher.set(filename, html)

        return html

    async def collect(self, base_url, queue_size=3):
        async def f(page):
            print(page)
            html, _ = await self.async_retry(3, self.get, f'{base_url}?p={page}')
            result = []
            for post_url in await self.run_in_executor(get_post_urls, html):
                _html = await self.get(post_url)
                urls = get_pict_urls(_html)
                for url in urls:
                    filename = urllib.parse.quote(url, safe='')
                    content, _ = self.cacher.get(filename, binary=True)
                    if not content:
                        await self.add_future('dlimage', download_file(url, filename, self.cacher))

                result.extend(urls)

            return len(result) > 0

        await self.queued_paging(1, 1000, lambda page: f(page), queue_size=queue_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('anicobin.ldblog.jp')
    parser.add_argument('--queue_size', type=int, default=3)
    parser.add_argument('--wait', '-w', type=int, default=10, help='wait time in seconds')
    parser.add_argument('--dir', '-d', type=str, default='cache', help='cache directory')
    parser.add_argument('--useragent', '-ua', type=str, default='')
    parser.add_argument('url', type=str)
    args = parser.parse_args()
    signal.signal(signal.SIGINT, lambda a, b: print('sigint') or exit(1))

    c = Anicobin(
        reporter=Reporter(1),
        waiter=Waiter([args.wait]),
        outdir=args.dir,
        useragent=args.useragent,
    )
    asyncio.run(c.run(c.collect(base_url=args.url, queue_size=args.queue_size)))
