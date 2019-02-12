from requests import Session
from typing import Iterable, List, NamedTuple, Optional, Sequence

Hash = str

class Item(NamedTuple):
    path: bytes
    meta: bytes
    blocks: Sequence[Hash]

class Cubic:
    _hfs_get: str
    _hfs_api: str
    _cubic_api: str
    _session: Session

    def __init__(self, user: str, token: str):
        ...

    def head_block(self, hash: Hash) -> Optional[bool]:
        ...

    def get_block(self, hash: Hash) -> Optional[bytes]:
        ...

    def post_block(self, block: bytes) -> Optional[Hash]:
        ...

    def get_tree(self) -> Optional[List[Item]]:
        ...

    def post_tree(self, put_items: Iterable[Item], delete_paths: Iterable[bytes]) -> Optional[str]:
        ...

    def bulk_head_block(self, hashes: Iterable[Hash]) -> List[Optional[bool]]:
        ...

    def bulk_get_block(self, hashes: Iterable[Hash]) -> List[Optional[bytes]]:
        ...

    def bulk_post_block(self, blocks: Iterable[bytes]) -> List[Optional[Hash]]:
        ...
