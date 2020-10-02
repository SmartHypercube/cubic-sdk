#!/usr/bin/env python3
from base64 import b64decode
import gzip
import pickle
import sys

from cubic import Item


def load_trees(path: str):
    with gzip.open(path, 'rb') as f:
        data = pickle.load(f)
    for tree, versions in data.items():
        last = set()
        for version, (add, remove) in versions.items():
            last |= add
            last -= remove
            items = []
            for line in last:
                path, meta, blocks = line.split(':')
                items.append(Item(
                    b64decode(path),
                    b64decode(meta),
                    blocks.split(',') if blocks else [],
                ))
            items.sort()
            yield tree, version, items


if __name__ == '__main__':
    for tree, version, items in load_trees(sys.argv[1]):
        print(f'{tree} - {version}: {len(items)} items')
