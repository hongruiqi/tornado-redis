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

class RedisClient(object):
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
        self._pending_cmd = deque()
        self._running = None
        self._callback = None
        self._decoder = None
        self._closed = False
        
    def __del__(self):
        if not self._closed:
            self.close()
    
    def close(self):
        self._closed = True
        stream = self._stream
        def release_stream():
            if self._running or len(self._pending_cmd)>0:
                ioloop.IOLoop.instance().add_callback(release_stream)
            else:
                stream 
                address = stream.socket.getpeername()
                self._connection_pool[address].append(stream)
                
    def connect(self):
        address = self.address
        # 若地址存在于 _connection_pool 字典中
        if address in self._connection_pool:
            pool = self._connection_pool[address]
            # 若连接池不为空
            if pool:
                return pool.popleft()  # 从连接池中取出一条连接
        else:
            self._connection_pool = deque() # 地址不存在时新建
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
        
        if not self._running and not self._connecting and len(self._pending_cmd)>0:
            self._running = True
            cmd, self._callback = self._pending_cmd.popleft()
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
            self._run()
        else:
            self.stream.read_until("\r\n", self.on_read_response)            
        
    def run(self, cmd, callback=None):
        if self._closed:
            raise RedisError("client has been closed")
        self._pending_cmd.append((cmd, callback))
        self._run()
    
class RedisChannel(object):
    def __init__(self, ip="127.0.0.1", port=6379, io_loop=None):
        self.address = (ip, port)
        self.io_loop = io_loop or ioloop.IOLoop.instance()
        self.channels = {}
        self._pending = deque()
        self.conn = _manager.get(self.address)
        self._closed = False
        self.listen()


    def execute(self, cmd, callback=None):
        print cmd
        if cmd[0] in ["subscribe", "psubscribe", "unsubscribe", "punsubscribe"]:
            for i in cmd[1:]:
                self._pending.append(callback)
        else:
            raise RedisError("command not supported")
        self.conn.stream.write(encode(cmd))

    @gen.engine
    def listen(self):
        conn = self.conn
        while not self._closed:
            s = yield gen.Task(conn.stream.read_until, "\r\n")
            g = decode(s[:-2])
            reply = g.next()
            while True:
                if isinstance(reply, Reply):
                    break
                s = yield gen.Task(conn.stream.read_until, "\r\n")
                reply = g.send(s[:-2])
            if isinstance(reply, ErrorReply):
                callback = self._pending.popleft()
                if callback:
                    callback(ErrorReply)
            elif isinstance(reply, MultiBulkReply):
                if reply.reply[0] in ["subscribe", "psubscribe"]:
                    callback = self._pending.popleft()
                    self.channels[(reply.reply[1], reply.reply[0][0]=="p")] = callback
                    assert len(self.channels)==int(reply.reply[2]), "channel num wrong"
                elif reply.reply[0] in ["unsubscribe", "punsubscribe"]:
                    self._pending.popleft()
                    if (reply.reply[1], reply.reply[0][0]=="p") in self.channels:
                        self.channels.pop((reply.reply[1], reply.reply[0][0]=="p"))
                    assert len(self.channels)==int(reply.reply[2]), "channel num wrong"
                elif reply.reply[0]=="message":
                    self.channels[(reply.reply[1], False)](reply.reply[2])
                elif reply.reply[0]=="pmessage":
                    self.channels[(reply.reply[1], True)](reply.reply[3])
            else:
                raise RedisError("reply type error")

    def close(self):
        def put_back():
            if len(self.channels):
                sub = []
                psub = []
                for k, v in self.channels.keys():
                    if v:
                        psub.append(k)
                    else:
                        sub.append(k)
                self.execute(["punsubscribe"] + psub)
                self.execute(["unsubscribe"] + sub)
                self.io_loop.add_timeout(time.time() + 5, put_back)
            else:
                self._closed = True
                _manager.put(self.conn)
        self.io_loop.add_callback(put_back)
