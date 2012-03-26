def encode(cmd):
    assert isinstance(cmd, (tuple, list)), "cmd to be encoded should be of type tuple or list"
    lines = []
    n = len(cmd)
    lines.append("*" + str(n))
    for c in cmd:
        c = str(c)
        lines.append("$" + str(len(c)))
        lines.append(c)
    lines.append("")
    result = "\r\n".join(lines)
    return result

class Reply(object):
    def __init__(self, reply):
        self.reply = reply

    def __str__(self):
        return str(self.reply)

class StatusReply(Reply): pass
class ErrorReply(Reply): pass
class IntegerReply(Reply): pass
class BulkReply(Reply): pass
class MultiBulkReply(Reply): pass

class DecodeError(Exception): pass

def decode(s):
    assert isinstance(s, str), "cmd to be decoded should be of type str"
    if s[0]=="+":
        yield StatusReply(s[1:])
    elif s[0]=="-":
        yield ErrorReply(s[1:])
    elif s[0]==":":
        yield IntegerReply(int(s[1:]))
    elif s[0]=="$":
        length = int(s[1:])
        if length==-1:
            yield BulkReply(None)
        else:
            s = yield
            if len(s)!=length:
                raise DecodeError("bulk length error")
            else:
                yield BulkReply(s)
    elif s[0]=="*":
        n = int(s[1:])
        if n==-1:
            yield MultiBulkReply(None)
        else:
            bulks = []
            for i in xrange(n):
                s = yield
                if s[0]=="$":
                    length = int(s[1:])
                    if length!=-1:
                        s = yield
                        if len(s)!=length:
                            raise DecodeError("bulk length error")
                        else:
                            bulks.append(s)
                    else:
                        bulks.append(None)
                elif s[0]==":":
                    bulks.append(int(s[1:]))
                else:
                    raise DecodeError("invalid bulk")
            yield MultiBulkReply(bulks)
    else:
        raise DecodeError("wrong reply format")
