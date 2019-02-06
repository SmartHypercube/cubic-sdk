from base64 import b64decode, b64encode
from hashlib import sha3_256
import requests


HASH = sha3_256
HASH_LEN = len(HASH().hexdigest())


def hash(data):
    return HASH(data).hexdigest()


def b64(data):
    return b64encode(data).decode('ascii')


class Node:
    def __init__(self, path, is_dir=False, meta=b'', children=(), blocks=()):
        """
        :param path: bytes
        :param is_dir: bool
        :param meta: bytes
        :param children: list of Nodes
        :param blocks: list of hashes
        """
        self.path = path
        self.is_dir = bool(is_dir)
        self.meta = meta
        self.children = list(children)
        self.blocks = list(blocks)

    @classmethod
    def from_json(cls, data):
        if not data['is_dir']:
            return cls(b64decode(data['path']), False, b64decode(data['meta']), blocks=data['blocks'])
        return cls(b64decode(data['path']), True, b64decode(data['meta']),
                   children=map(cls.from_json, data['children']))

    def __str__(self):
        return self.path.decode('utf8', errors='ignore')


class CubicServer:
    class Error(Exception):
        pass

    def __init__(self, url):
        self.url = url
        self.session = requests.Session()

    def __call__(self, api, **kwargs):
        response = self.session.post(self.url, json=dict(api=api, **kwargs)).json()
        if response['error'] is None:
            return response['result']
        raise self.Error(response['error'])

    def check_hash(self, hash_list):
        """
        :param hash_list: list of hashes
        :return: hashes which can be skipped
        """
        return self('check_hash', l=list(hash_list))

    def put_block(self, blocks):
        """
        :param blocks: dict of (hash, bytes)
        :return: None
        """
        return self('put_block', l=[{'hash': h, 'data': b64(blocks[h])} for h in blocks])

    def update_tree(self, add, remove):
        """
        :param add: list of Nodes
        :param remove: list of Nodes
        :return: None
        """
        return self(
            'update_tree',
            add=[{'path': b64(n.path), 'is_dir': n.is_dir, 'meta': b64(n.meta), 'blocks': n.blocks} for n in add],
            remove=[{'path': b64(n.path)} for n in remove],
        )

    def get_tree(self, base):
        """
        :param base: Node
        :return: Node
        """
        return Node.from_json(self('get_tree', base=b64(base.path)))

    def get_block(self, l):
        """
        :param l: list of hashes
        :return: dict of (hash, bytes)
        """
        return {h: b64decode(d) for h, d in self('get_block', l=list(l)).items()}

    def reset(self):
        """
        :return: None
        """
        return self('reset')
