from pyol.config import Config


def test_config():
    c = Config()
    d = c.to_dict()
    e = c.from_dict(d)
    assert c == e


test_config()