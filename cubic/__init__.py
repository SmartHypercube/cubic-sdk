"""Cubic SDK"""

__all__ = ['HASH', 'HASH_LEN', 'Item', 'Cubic']

from base64 import b64decode, b64encode
from collections import namedtuple
from hashlib import sha3_256
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter

HASH = sha3_256
HASH_LEN = len(HASH().hexdigest())

Item = namedtuple('Item', ('path', 'meta', 'blocks'))


def async_map(func, iterable):
    from asyncio import get_event_loop
    from concurrent.futures import ThreadPoolExecutor

    loop = get_event_loop()
    executor = ThreadPoolExecutor(20)

    async def coroutine():
        futures = [loop.run_in_executor(executor, func, i) for i in iterable]
        return [await f for f in futures]

    return loop.run_until_complete(coroutine())


class Cubic:
    _hfs_get = 'https://0x01-hfs.oss-cn-shenzhen.aliyuncs.com/'
    _hfs_api = 'https://1802550410649663.cn-shenzhen.fc.aliyuncs.com' \
               '/2016-08-15/proxy/hfs/hfs/'
    _cubic_api = 'https://1802550410649663.cn-shenzhen.fc.aliyuncs.com' \
                 '/2016-08-15/proxy/cubic/cubic/'
    _temp_put = 'https://0x01-temp.oss-cn-shenzhen.aliyuncs.com/'

    class Error(Exception):
        pass

    def __init__(self, user, token, session=None):
        if session is None:
            session = requests.Session()
            session.mount('https://', HTTPAdapter(100, 100))
            session.mount('http://', HTTPAdapter(100, 100))
        self._session = session
        self._cubic_api += user
        self._auth = (user.partition('.')[0], token)

    def head_block(self, key):
        res = self._session.head(self._hfs_get + key)
        return res.ok

    def get_block(self, key):
        res = self._session.get(self._hfs_get + key)
        if not res.ok:
            raise Cubic.Error(res)
        return res.content

    def post_block(self, block, key=None):
        url = self._temp_put + str(uuid4())
        res = self._session.put(url, block)
        if not res.ok:
            raise Cubic.Error(res)
        res = self._session.post(self._hfs_api, json={'key': key, 'url': url})
        if not res.ok:
            raise Cubic.Error(res)
        return res.text.strip().rpartition('/')[2]

    def put_block(self, key, block):
        self.post_block(block, key)

    def get_tree(self):
        res = self._session.get(self._cubic_api, auth=self._auth, stream=True)
        if not res.ok:
            raise Cubic.Error(res)
        for line in res.iter_lines():
            path, meta, blocks = line.strip().split(b':')
            yield Item(b64decode(path),
                       b64decode(meta),
                       blocks.decode('ascii').split(',') if blocks else [])

    def _post_tree(self, put_items=(), delete_paths=(), base=''):
        def stream():
            paths = set()
            for path, meta, blocks in put_items:
                if path in paths:
                    raise ValueError(f'duplicate path {repr(path)}')
                paths.add(path)
                yield b':'.join((b64encode(path),
                                 b64encode(meta),
                                 ','.join(blocks).encode('ascii'))) + b'\n'
            for path in delete_paths:
                if path in paths:
                    raise ValueError(f'duplicate path {repr(path)}')
                paths.add(path)
                yield b64encode(path) + b'\n'

        url = self._temp_put + str(uuid4())
        res = self._session.put(url, b''.join(stream()))
        if not res.ok:
            raise Cubic.Error(res)
        res = self._session.post(self._cubic_api, auth=self._auth,
                                 json={'changes': url, 'base': base})
        if not res.ok:
            raise Cubic.Error(res)

    def post_tree(self, put_items=(), delete_paths=()):
        self._post_tree(put_items, delete_paths)

    def put_tree(self, items=()):
        self._post_tree(items, base='null')

    def delete_tree(self):
        self._post_tree(base='null')

    def bulk_head_block(self, hashes):
        """Calls head_block in bulk."""
        return async_map(self.head_block, hashes)

    def bulk_get_block(self, hashes):
        """Calls get_block in bulk."""
        return async_map(self.get_block, hashes)

    def bulk_post_block(self, blocks):
        """Calls post_block in bulk."""
        return async_map(self.post_block, blocks)
