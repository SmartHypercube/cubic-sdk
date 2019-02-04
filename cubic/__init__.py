from hashlib import sha3_256
from socket import socket

HASH = sha3_256
HASH_LEN = len(HASH().hexdigest())


class Request:
    def __init__(self, server, port=8080):
        self.conn = socket()
        self.conn.connect((server, port))

    def close(self):
        self.conn.close()


class CheckHashRequest(Request):
    def send_bulk(self, hash_list):
        self.conn.send(('\n'.join(hash_list) + '\n').encode('ascii'))
        for i, hash in enumerate(hash_list):
            self.conn.send(f'{hash}\n'.encode('ascii'))

    def recv_bulk(self):
        return self.conn.recv(65536).decode('ascii').splitlines()


class SendBlockRequest(Request):
    def send_one(self, hash, block):
        self.conn.send(f'{hash}:{len(block)}\n'.encode('ascii'))
        self.conn.send(block)
        self.conn.send(b'\n')

    def recv_bulk(self):
        return self.conn.recv(65536).decode('ascii').splitlines()
