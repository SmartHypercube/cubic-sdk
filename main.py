from cubic import CubicServer, hash, Node


def main():
    s = CubicServer('http://localhost:8000/')
    ds = [bytes((i,)) for i in range(20)]
    dd = {hash(d): d for d in ds}
    for h in s.check_hash(dd):
        del dd[h]
    s.put_block(dd)
    dk = list(dd.keys())
    s.update_tree([Node(b'/a/b', blocks=dk[:3])], [])
    s.update_tree([Node(b'/a/c', blocks=dk[:3])], [])
    s.update_tree([Node(b'/d', blocks=dk[:3])], [])
    s.update_tree([], [Node(b'/a/b')])
    print(*s.get_tree(Node(b'/a')).children)
    s.update_tree([], [Node(b'/a')])
    s.update_tree([], [Node(b'/d')])


if __name__ == '__main__':
    main()
