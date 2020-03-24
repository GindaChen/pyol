from pyol.config import *


def test_config():
    c = Config()
    d = c.to_dict()
    e = c.from_dict(d)
    assert c == e

def test_push_pop():
    c = Config()
    c.push(worker_port="49000")
    print(c)
    c.push(worker_port="49001", server_mode=ServerMode.lambda_)
    print(c)
    c.pop()
    print(c)
    c.pop()
    print(c)
    c.pop()
    print(c)




# test_config()
test_push_pop()