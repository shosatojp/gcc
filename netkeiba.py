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


def get_race_urls(html):
    doc = bs4.BeautifulSoup(html, 'lxml')
    return [
        'https://db.netkeiba.com' + tr.select('td')[4].select_one('a')['href']
        for tr in doc.select('table.race_table_01 tr')[1:]
    ]


def get_horse_urls(html):
    doc = bs4.BeautifulSoup(html, 'lxml')
    return [
        'https://db.netkeiba.com' + tr.select('td')[1].select_one('a')['href']
        for tr in doc.select('table.race_table_01 tr')[1:]
    ]


def get_nextpage_data(html):
    doc = bs4.BeautifulSoup(html, 'lxml')
    return {
        input_elem['name']: input_elem['value']
        for input_elem
        in doc.select('form[name=sort] > input')
    }


class Keiba(Collector):
    def __init__(self, reporter, waiter, outdir, useragent) -> None:
        super(Keiba, self).__init__()
        self.reporter: Reporter = reporter
        self.waiter = waiter
        self.outdir = outdir
        self.useragent = useragent
        self.cacher = Cacher(self.outdir)
        self.semaphore = Semaphore(2)

    async def get_search_page(self, n: int, options: dict = {
        'pid': str,
        'word': str,
        'track[]': str,
        'start_year': str,
        'start_mon': str,
        'end_year': str,
        'end_mon': str,
        'jyo[]': str,
        'kyori_min': str,
        'kyori_max': str,
        'sort': str,
        'list': str,
    }):
        url = 'https://db.netkeiba.com/'
        psuedo_url = f'{url}?{urllib.parse.urlencode(options)}&page=1'
        filename = urllib.parse.quote(psuedo_url + '.html', safe='')

        cache, _ = self.cacher.get(filename)
        if cache:
            search_result = cache
        else:
            await self.waiter.wait(psuedo_url)
            print('fetching', psuedo_url)
            async with aiohttp.request(
                'post',
                url=url,
                headers={'content-type': 'application/x-www-form-urlencoded',
                         'user-agent': self.useragent},
                data=urllib.parse.urlencode(options)
            ) as req:
                req.encoding = 'euc-jp'
                search_result = await req.text()
                if str(req.url) == url:
                    self.cacher.set(filename, search_result)
                else:
                    print(f'Warning: redirected to {str(req.url)}')
                    return None

        if n == 1:
            return search_result
        else:
            data = await self.run_in_executor(get_nextpage_data, search_result)
            data['page'] = str(n)
            psuedo_url = f'{url}?{urllib.parse.urlencode(options)}&page={n}'
            filename = urllib.parse.quote(psuedo_url + '.html', safe='')

            cache, _ = self.cacher.get(filename)
            if cache:
                result = cache
            else:
                await self.waiter.wait(psuedo_url)
                print('fetching', psuedo_url)
                async with aiohttp.request(
                    'post',
                    url=url,
                    headers={'content-type': 'application/x-www-form-urlencoded',
                             'user-agent': self.useragent},
                    data=urllib.parse.urlencode(data, encoding='euc-jp')
                ) as req:
                    try:
                        req.encoding = 'euc-jp'
                        result = await req.text()
                        self.cacher.set(filename, result)
                    except Exception as e:
                        print(e)
                        return None
            return result

    async def get_race_page(self, url):
        filename = urllib.parse.quote(url, safe='') + '.html'
        cache, _ = self.cacher.get(filename, ext='')
        if cache:
            html = cache
        else:
            await self.waiter.wait(url)
            print('fetching', url)
            async with aiohttp.request('get', url, headers={'user-agent': self.useragent}) as req:
                req.encoding = 'euc-jp'
                html = await req.text()
                self.cacher.set(filename, html)

        return html

    async def collect(self, year, queue_size=3):
        async def f(page):
            print(page)
            html, _ = await self.async_retry(3, self.get_search_page, page, {
                'pid': 'race_list',
                'start_year': str(year),
                'end_year': str(year),
                'sort': 'date',
                'list': '100'
            })
            return len([
                await self.add_future('get_race', self.get_race_page(race_url))
                for race_url in await self.run_in_executor(get_race_urls, html)
            ]) == 100 if html else False

        await self.queued_paging(1, 1000, lambda page: f(page), queue_size=queue_size)

    async def collect_horse(self, year, queue_size=3):
        async def f(page):
            print(page)
            html, error = await self.async_retry(3, self.get_search_page, page, {
                'pid': 'horse_list',
                'list': '100',
                'birthyear': year,
            })
            if error:
                print('Waringn: max retries exceeded')
                return False
            return len(await self.run_in_executor(get_horse_urls, html)) == 100 if html else False

        await self.queued_paging(1, 1000, lambda page: f(page), queue_size=queue_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('netkeiba.com')
    parser.add_argument('--type', '-t', choices=['race', 'horse'], default='race')
    parser.add_argument('--year', '-y', type=int, required=True)
    parser.add_argument('--queue_size', type=int, default=3)
    parser.add_argument('--wait', '-w', type=int, default=10, help='wait time in seconds')
    parser.add_argument('--dir', '-d', type=str, default='cache', help='cache directory')
    parser.add_argument('--useragent', '-ua', type=str, default='')
    args = parser.parse_args()
    signal.signal(signal.SIGINT, lambda a, b: print('sigint') or exit(1))

    c = Keiba(
        reporter=Reporter(1),
        waiter=Waiter([args.wait]),
        outdir=args.dir,
        useragent=args.useragent,
    )
    if args.type == 'race':
        asyncio.run(c.run(c.collect(args.year, queue_size=args.queue_size)))
    elif args.type == 'horse':
        asyncio.run(c.run(c.collect_horse(args.year, queue_size=args.queue_size)))
    else:
        print('invalid type')
        exit(1)
