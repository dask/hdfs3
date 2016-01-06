from hdfs3 import HDFileSystem
import pytest


@pytest.yield_fixture
def hdfs():
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.ls('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    yield hdfs

    if hdfs.ls('/tmp/test'):
        hdfs.rm('/tmp/test')


def test_example(hdfs):
    data = b'a' * (10 * 2**20)

    with hdfs.open('/tmp/test/file', 'w', repl=1) as f:
        f.write(data)

    with hdfs.open('/tmp/test/file', 'r') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_ls_touch(hdfs):
    assert not hdfs.ls('/tmp/test')
    hdfs.touch('/tmp/test/1')
    hdfs.touch('/tmp/test/2')
    L = hdfs.ls('/tmp/test')
    assert set(d['name'] for d in L) == {'/tmp/test/1', '/tmp/test/2'}


def test_rm(hdfs):
    assert not hdfs.ls('/tmp/test')
    hdfs.touch('/tmp/test/1')
    assert hdfs.ls('/tmp/test/1')
    hdfs.rm('/tmp/test/1')
    assert not hdfs.ls('/tmp/test')


def test_pickle(hdfs):
    data = b'a' * (10 * 2**20)
    with hdfs.open('/tmp/test/file3', 'w', repl=1) as f:
        f.write(data)

    assert hdfs._handle > 0
    import pickle
    hdfs2 = pickle.loads(pickle.dumps(hdfs))
    assert hdfs2._handle > 0

    hdfs2.touch('/tmp/test/file')
    hdfs2.ls('/tmp/test/file')

    with hdfs2.open('/tmp/test/file2', 'w', repl=1) as f:
        f.write(data)

    with hdfs2.open('/tmp/test/file2', 'r') as f:
        f.seek(5)
        f.read(10)

    with hdfs.open('/tmp/test/file4', 'w', repl=1) as f:
        f.write(data)
