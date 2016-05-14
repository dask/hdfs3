from hdfs3.tests.test_hdfs3 import hdfs
from hdfs3.mapping import HDFSMap

root = '/tmp/mapping'


def test_simple(hdfs):
    mw = HDFSMap(hdfs, root)
    mw.clear()
    assert not mw

    assert list(mw) == list(mw.keys()) == []
    assert list(mw.values()) == []
    assert list(mw.items()) == []


def test_with_data(hdfs):
    mw = HDFSMap(hdfs, root)
    mw['x'] = b'123'
    assert list(mw) == list(mw.keys()) == ['x']
    assert list(mw.values()) == [b'123']
    assert list(mw.items()) == [('x', b'123')]
    assert mw['x'] == b'123'
    assert bool(mw)

    assert set(hdfs.walk(root)) == {root+'/x', root}
    mw['x'] = b'000'
    assert mw['x'] == b'000'

    mw['y'] = b'456'
    assert mw['y'] == b'456'
    assert set(mw) == {'x', 'y'}

    mw.clear()
    assert list(mw) == []


def test_complex_keys(hdfs):
    mw = HDFSMap(hdfs, root)
    mw[1] = b'hello'
    assert mw[1] == b'hello'
    del mw[1]

    mw[1, 2] = b'world'
    assert mw[1, 2] == b'world'
    del mw[1, 2]

    mw['x', 1, 2] = b'hello world'
    assert mw['x', 1, 2] == b'hello world'
    print(list(mw))

    assert ('x', 1, 2) in mw


def test_pickle(hdfs):
    d = HDFSMap(hdfs, root)
    d['x'] = b'1'

    import pickle
    d2 = pickle.loads(pickle.dumps(d))

    assert d2['x'] == b'1'


def test_array(hdfs):
    from array import array
    d = HDFSMap(hdfs, root)
    d['x'] = array('B', [65] * 1000)

    assert d['x'] == b'A' * 1000


def test_bytearray(hdfs):
    from array import array
    d = HDFSMap(hdfs, root)
    d['x'] = bytearray(b'123')

    assert d['x'] == b'123'
