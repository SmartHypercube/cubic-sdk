from base64 import b64decode, b64encode
from collections import namedtuple
from hashlib import sha3_256
import requests

__all__ = ['HASH', 'HASH_LEN', 'Cubic', 'Item']

HASH = sha3_256
HASH_LEN = len(HASH().hexdigest())

Item = namedtuple('Item', ('path', 'meta', 'blocks'))


def b64(data):
    return b64encode(data).decode('ascii')


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
    _hfs_get = 'https://hfs-1251965935.cos.ap-shanghai.myqcloud.com/'
    _hfs_api = 'https://service-1p2s8p0h-1251965935.ap-shanghai.apigateway.myqcloud.com/release/'
    _cubic_api = 'https://service-qkomvv9d-1251965935.ap-shanghai.apigateway.myqcloud.com/release/'

    def __init__(self, user, token):
        self._session = requests.Session()
        self._cubic_api += f'{user}/{token}'

    def head_block(self, hash):
        """Returns True if server has the block. False if not. None if request failed."""
        res = self._session.head(self._hfs_get + hash)
        return res.ok if res.ok or res.status_code == 404 else None

    def get_block(self, hash):
        """Returns block in bytes. None if request failed."""
        res = self._session.get(self._hfs_get + hash)
        return res.content if res.ok else None

    def post_block(self, block):
        """Uploads block in bytes. Returns the hash. None if request failed."""
        res = self._session.post(self._hfs_api, b64encode(block))
        return res.text if res.ok else None

    def get_tree(self):
        """Returns Items in the tree. None if request failed."""
        res = self._session.get(self._cubic_api)
        if not res.ok:
            return
        items = []
        for line in res.iter_lines():
            path, meta, blocks = line.split(b':')
            items.append(Item(b64decode(path), b64decode(meta), blocks.decode('ascii').split(',')))
        return items

    def post_tree(self, put_items=(), delete_paths=()):
        """Updates the tree. Returns the new version ID. None if request failed."""
        res = self._session.post(self._cubic_api, json={
            'put_list': [{
                'path': b64(i.path),
                'meta': b64(i.meta),
                'blocks': list(i.blocks)} for i in put_items],
            'delete_list': [{'path': b64(p)} for p in delete_paths],
        })
        return res.text if res.ok else None

    def bulk_head_block(self, hashes):
        """Calls head_block in bulk."""
        return async_map(self.head_block, hashes)

    def bulk_get_block(self, hashes):
        """Calls get_block in bulk."""
        return async_map(self.get_block, hashes)

    def bulk_post_block(self, blocks):
        """Calls post_block in bulk."""
        return async_map(self.post_block, blocks)
