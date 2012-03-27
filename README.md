tornado-redis
================================

An redis client for tornado.

Example
-------

    from redis import RedisClient
    client = RedisClient()
    client.execute(["set", "foo", "bar"])

Requirements
------------
The following python libraries are required

* [tornado](http://github.com/facebook/tornado)
