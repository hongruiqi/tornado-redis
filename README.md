tornado-redis
================================

An redis client for tornado.

example:

    from redis import RedisClient
    client = RedisClient()
    client.execute(["set", "foo", "bar"])
