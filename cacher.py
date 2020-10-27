import shutil
import os
import json


def tmp_save(path: str, content: str):
    tmppath = path+'~'
    with open(tmppath, 'wt', encoding='utf-8') as f:
        f.write(content)
    shutil.move(tmppath, path)


class Cacher():
    def __init__(self, cache_dir: str = 'cache') -> None:
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        self.cache_dir = cache_dir

    def get(self, filename: str):
        content, info = None, None

        path = os.path.join(self.cache_dir, filename)
        if os.path.exists(path):
            with open(path, 'rt', encoding='utf-8') as f:
                content = f.read()

        infopath = os.path.join(self.cache_dir, filename+'.json')
        if os.path.exists(infopath):
            with open(infopath, 'rt', encoding='utf-8') as f:
                info = json.loads(f.read())

        return content, info

    def set(self, filename: str, content, info: dict = None):
        path = os.path.join(self.cache_dir, filename)
        tmp_save(path, content)

        if info:
            infopath = os.path.join(self.cache_dir, filename+'.json')
            tmp_save(infopath, json.dumps(info))
