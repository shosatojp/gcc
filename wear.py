import json
import os
import argparse
from asyncio.locks import Semaphore
from collector import Collector
import asyncio
from downloader import Downloader
from reporter import Reporter, INFO, NETWORK
from cacher import Cacher, tmp_save
from waiter import Waiter
import aiohttp
import bs4
import urllib.parse


def parse_gallely(html, userdata):
    '''
    ギャラリーページからデータと画像URL取得
    別プロセスで処理
    '''
    doc = bs4.BeautifulSoup(html, 'lxml')
    results = []
    for e in doc.select('.like_mark'):
        link = 'https:' + e.select_one('.img img')['data-originalretina']
        data = {
            'snapid': e['data-snapid'],
            'saves': int(e.select_one('.btn_save span').text.strip()),
            'likes': int(e.select_one('.btn_like span').text.strip()),
            'link': e.select_one('.over')['href']
        }

        # データが渡されたときはつける
        if userdata:
            data['user'] = userdata

        # first name if exists
        elem_first_name = e.select_one('.namefirst')
        if elem_first_name:
            data['first_name'] = elem_first_name.text.strip()

        # height if exists
        elem_height = e.select_one('.height')
        if elem_height:
            data['height'] = elem_height.text.strip()
        data['url'] = link

        results.append((link, data))

    return results


def parse_user(html):
    '''
    ユーザー一覧ページから各ユーザーページへのリンクとユーザーデータ取得
    別プロセスで処理
    '''
    results = []
    doc = bs4.BeautifulSoup(html, 'lxml')
    for e in doc.select('#list_1column li.list'):
        link = 'https://wear.jp' + e.select_one('.over')['href']
        type_e = e.select_one('h3.name span')
        if not type_e:
            user_type = 'normal'
        elif 'wearista' in type_e['class']:
            user_type = 'wearista'
        elif 'shopstaff' in type_e['class']:
            user_type = 'shopstaff'
        else:
            user_type = ''

        shopname_e = e.select_one('.shopname')
        if not shopname_e:
            shopname = ''
        else:
            shopname = shopname_e.text.strip()

        data = {
            'userid': e.select_one('.over')['href'].replace('/', ''),
            'name': e.select_one('h3.name').text.strip(),
            'info': list(map(lambda li: li.text.strip(), e.select('ul.info li'))),
            'meta': list(map(lambda li: li.text.strip(), e.select('ul.meta li'))),
            'brands': list(map(lambda li: li.text.strip(), e.select('.fav_brand ul li'))),
            'user_type': user_type,
            'shopname': shopname,
        }

        results.append((link, data))

    return results


class WearCollector(Collector):
    def __init__(self,
                 reporter: Reporter,
                 waiter: Waiter,
                 outdir: str,
                 useragent: str = ''):
        super(WearCollector, self).__init__()
        self.reporter: Reporter = reporter
        self.waiter = waiter
        self.outdir = outdir
        self.useragent = useragent
        self.cacher = Cacher(self.outdir)
        # 非同期処理の同時接続数制御
        self.semaphore = Semaphore(2)
        # ファイルダウンローダ
        self.downloader = Downloader(self.waiter, self.semaphore, self.reporter)

    async def download_user_page(self, url: str, page_num):
        url = url + f'?pageno={page_num}'

        # キャッシュがあれば使う
        filename = urllib.parse.quote(url, safe='') + '.html'
        content, info = self.cacher.get(filename)
        if content and info:
            html = content
            realurl = info.get('realurl')
            self.reporter.report(INFO, f'use cache {url}')
        else:
            await self.waiter.wait(url)
            async with self.semaphore:
                self.reporter.report(INFO, f'fetching {url}', type=NETWORK)
                async with aiohttp.request('get', url, headers={'user-agent': self.useragent}) as res:
                    html = await res.text()
                    realurl = str(res.url)
                    self.cacher.set(filename, html, {'status': res.status, 'realurl': realurl})

        # 終了条件
        if page_num >= 2 and realurl.count('?pageno') == 0:
            return False
        else:
            for url, data in await self.run_in_executor(parse_user, html):
                await self.add_future('gallery', self.gallery_collector(url, 1, 501, userdata=data))
            return True

    async def user_collector(self, url: str, pagestart: int, pageend: int):
        await self.queued_paging(
            pagestart, pageend,
            lambda page: self.download_user_page(url, page))

    async def download_gallery_page(self, url: str, page_num: int, userdata=None):
        url = url + f'?pageno={page_num}'
        filename = urllib.parse.quote(url, safe='') + '.html'
        content, info = self.cacher.get(filename)
        if content and info:
            html = content
            realurl = info.get('realurl')
            self.reporter.report(INFO, f'use cache {url}')
        else:
            await self.waiter.wait(url)
            async with self.semaphore:
                self.reporter.report(INFO, f'fetching {url}', type=NETWORK)
                async with aiohttp.request('get', url, headers={'user-agent': self.useragent}) as res:
                    html = await res.text()
                    realurl = str(res.url)
                    self.cacher.set(filename, html, {'status': res.status, 'realurl': realurl})

        # 終了条件
        if page_num >= 2 and realurl.count('?pageno') == 0:
            return False
        else:
            for url, data in await self.run_in_executor(parse_gallely, html, userdata):
                imagefile = urllib.parse.quote(url, safe='')
                tmp_save(os.path.join(self.outdir, imagefile+'.json'), json.dumps(data))
                imagepath = os.path.join(self.outdir, imagefile)
                if not os.path.exists(imagepath):
                    await self.add_future('image', self.downloader.download_file(
                        url, imagepath, headers={'user-agent': self.useragent}))
            return True

    async def gallery_collector(self, url: str, pagestart: int, pageend: int, userdata=None):
        await self.queued_paging(
            pagestart, pageend,
            lambda page: self.download_gallery_page(url, page, userdata=userdata))


if __name__ == "__main__":

    parser = argparse.ArgumentParser('wear')
    parser.add_argument('url', action='store', help='URL')
    parser.add_argument('--pagestart', '-ps', type=int, required=True)
    parser.add_argument('--pageend', '-pe', type=int, required=True)
    parser.add_argument('--useragent', '-ua', type=str, default='')
    parser.add_argument('--waitlist', '-wl', type=str, default='wait.json')
    parser.add_argument('--outdir', '-o', type=str, required=True)
    parser.add_argument('--loglevel', '-ll', default=2, type=int, help='log level')
    parser.add_argument('--wait', '-w', default='5', nargs='+', type=str, help='interval for http requests. default is none. `-w 0.5` `-w random 1 2.5`')
    args = parser.parse_args()

    c = WearCollector(
        reporter=Reporter(args.loglevel),
        waiter=Waiter(args.wait, args.waitlist),
        outdir=args.outdir,
        useragent=args.useragent
    )
    asyncio.run(
        c.run(c.user_collector(args.url, args.pagestart, args.pageend))
    )
