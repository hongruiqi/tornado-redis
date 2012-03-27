# -*- coding:utf-8 -*-
import socket
from collections import deque
from functools import partial
from tornado import ioloop
from tornado import iostream
from tornado import gen
from encode import *
from contextlib import contextmanager
import time

class RedisError(Exception): pass

class Client(object):
    """RedisClient

    * 管理连接池.
      一个 RedisClient 实例唯一使用一条 redis 连接
    * 执行redis命令.
    """

    # 连接池保存在 _connection_pool 字典中
    # key 为连接的目标 ip 和端口， value 为
    # 每个（ip， 端口）对应的连接链表
    _connection_pool = {}

    def __init__(self, ip="127.0.0.1", port=6379):
        self.address = (ip, port)
        self._stream = None
        self._connecting = False
        self._cmd_queue = deque()
        self._running = None
        self._callback = None
        self._decoder = None
        self._closed = False

    def __del__(self):
        if not self._closed:
            self.close()

    def close(self):
        self._closed = True
        def release_stream():
            if self._running or len(self._cmd_queue)>0:
                ioloop.IOLoop.instance().add_callback(release_stream)
            else:
                self._stream = None
                address = self.stream.socket.getpeername()
                self._connection_pool[address].append(self.stream)
        release_stream()

    def connect(self):
        address = self.address
        # 若地址存在于 _connection_pool 字典中
        if address in self._connection_pool:
            pool = self._connection_pool[address]
            # 若连接池不为空
            if pool:
                return pool.popleft()  # 从连接池中取出一条连接
        else:
            self._connection_pool[address] = deque() # 地址不存在时新建
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._stream = iostream.IOStream(s)
        self._stream.connect(address, self._on_connect)
        self._connecting = True
        self._stream.set_close_callback(partial(self._on_stream_close, self._stream))

    def _on_connect(self):
        self._connecting = False
        self._run()

    def _on_stream_close(self, stream):
        if self._stream is stream:
            self._stream = None
        address = stream.socket.getpeername()
        self._connection_pool[address].remove(stream)

    def _run(self):
        if not self._stream:
            self.connect()
            return

        if not self._running and not self._connecting and len(self._cmd_queue)>0:
            self._running = True
            cmd, self._callback = self._cmd_queue.popleft()
            self._decoder = False
            self.stream.write(encode(cmd))
            self.stream.read_until("\r\n", self._on_read_response)

    def _run_callback(self, data):
        callback = self._callback
        self._callback = None
        if callback:
            callback(data)

    def _on_read_response(self, data):
        g = self.decoder
        if not g:
            g = decode(data[:-2])
            self.decoder = g
            t = g.next()
        else:
            t = g.send(data[:-2])
        if isinstance(t, Reply):
            self._callback(Reply)
            self._on_response_finish()
        else:
            self.stream.read_until("\r\n", self.on_read_response)
    
    def _on_response_finish(self):
        self._running = False
        self._run()

    def run(self, cmd, callback=None):
        if self._closed:
            raise RedisError("client has been closed")
        self._cmd_queue.append((cmd, callback))
        self._run()
        
class PipeLine(Client):    
    def __init__(self, ip="127.0.0.1", port=6379):
        self.address = (ip, port)
        self._stream = None
        self._connecting = False
        self._cmd_queue = None
        self._pending_cmd_queues = deque()
        self._pending_cmd_queue = deque()
        self._running = None
        self._callback = None
        self._decoder = None
        self._closed = False
    
    def append(self):
        if self._closed:
            raise RedisError("client has been closed")
        self._pending_cmd_queue.append((cmd, callback))
    
    def run(self):
        self._pending_cmd_queues.append(self._pending_cmd_queue)
        self._run_queue()
        
    def _run_queue(self):
        if self._running or len(self,_cmd_queue)>0:
            return
        if len(self._pending_cmd_queues)>0:
            self._cmd_queue = self.pending_cmd_queues.popleft()
            self._run()
    
    def _on_response_finish(self):
        self._running = False
        if len(self._cmd_queues)==0:
            self._run_queue()
            return
        self._run()
        
    # TODO: close 函数是否需要修改？
        