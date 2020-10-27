from asyncio.locks import Semaphore
from wsgiref import headers
from reporter import ERROR, INFO, NETWORK, Reporter
import aiofiles
import shutil
from waiter import Waiter
import aiohttp


class Downloader():
    def __init__(self, waiter: Waiter, semaphore: Semaphore, reporter: Reporter) -> None:
        self.waiter = waiter
        self.semaphore = semaphore
        self.reporter = reporter

    async def download_file(self, url: str, path: str, headers={}):
        try:
            await self.waiter.wait(url)
            async with self.semaphore:
                async with aiohttp.request('GET', url, headers=headers) as res:
                    if res.status == 200:
                        self.reporter.report(INFO, f'downloading {url} -> {path}', type=NETWORK)

                        temppath = path + '~'
                        async with aiofiles.open(temppath, 'wb') as fd:
                            while True:
                                chunk = await res.content.read(1024)
                                if not chunk:
                                    break
                                await fd.write(chunk)
                        shutil.move(temppath, path)
                    else:
                        self.reporter.report(ERROR, f'download_img: {res.status} {url}', type=NETWORK)
        except Exception as e:
            self.reporter.report(ERROR, f'download_img: {url} {e}', type=NETWORK)
