from hdfs3 import HDFileSystem, lib
import pytest
import ctypes

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
        assert f._handle

    with hdfs2.open('/tmp/test/file2', 'r') as f:
        f.seek(5)
        f.read(10)
        assert f._handle

    with hdfs.open('/tmp/test/file4', 'w', repl=1) as f:
        f.write(data)
        assert f._handle


def test_libload():
    assert lib.hdfsGetLastError()
    assert len(lib.hdfsGetLastError.__doc__) > 0
    assert lib.hdfsFileIsOpenForRead(lib.hdfsFile()) == False


def test_bad_open(hdfs):
    with pytest.raises(IOError):
        hdfs.open('')


def test_write_blocksize(hdfs):
    fn = '/tmp/test/file'
    with hdfs.open(fn, 'w', block_size=10) as f:
        f.write(b'a' * 25)

    blocks = hdfs.get_block_locations(fn)
    assert len(blocks) == 3
    assert blocks[0]['length'] == 10
    assert blocks[1]['length'] == 10
    assert blocks[2]['length'] == 5

    with pytest.raises(ValueError):
        hdfs.open(fn, 'r', block_size=123)


def test_glob(hdfs):
    filenames = ['a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
    filenames = ['/tmp/test/' + s for s in filenames]
    for fn in filenames:
        hdfs.touch(fn)

    assert set(hdfs.glob('/tmp/test/a*')) == ['/tmp/test/' + a
                                              for a in ['a1', 'a2', 'a3']]
    assert len(hdfs.glob('/tmp/test/c/')) == 3
    assert set(hdfs.glob('/tmp/test/')) == set(filenames)


def test_info(hdfs):
    fn = '/tmp/test/foo'
    with hdfs.open(fn, 'w', repl=1, block_size=100000000) as f:
        f.write('a' * 5)

    info = hdfs.info(fn)
    assert info['size'] == 5
    assert info['name'] == fn
    assert info['replication'] == 1
    assert info['block_size'] == 100000000


def test_df(hdfs):
    with hdfs.open('/tmp/test/file1', 'w', repl=1) as f:
        f.write('a' * 10)
    with hdfs.open('/tmp/test/file2', 'w', repl=1) as f:
        f.write('a' * 10)

    result = hdfs.df()
    assert result['capacity'] > result['used']


a = '/tmp/test/a'
b = '/tmp/test/b'

def test_move(hdfs):
    hdfs.touch(a)
    assert hdfs.ls(a)
    assert not hdfs.ls(b)
    hdfs.mv(a, b)
    assert not hdfs.ls(a)
    assert hdfs.ls(b)


def test_copy(hdfs):
    hdfs.touch(a)
    assert hdfs.ls(a)
    assert not hdfs.ls(b)
    hdfs.cp(a, b)
    assert hdfs.ls(a)
    assert hdfs.ls(b)


def test_exists(hdfs):
    assert not hdfs.exists(a)
    hdfs.touch(a)
    assert hdfs.exists(a)
    hdfs.rm(a)
    assert not hdfs.exists(a)


def test_full_read(hdfs):
    with hdfs.open(a, 'w') as f:
        f.write(b'0123456789')

    with hdfs.open(a, 'r') as f:
        assert len(f.read(4)) == 4
        assert len(f.read(4)) == 4
        assert len(f.read(4)) == 2

    with hdfs.open(a, 'r') as f:
        assert len(f.read()) == 10

    with hdfs.open(a, 'r') as f:
        assert f.tell() == 0
        f.seek(3)
        assert f.read(4) == b'3456'
        assert f.tell() == 7
        assert f.read(4) == b'789'
        assert f.tell() == 10

def test_tail(hdfs):
    with hdfs.open(a, 'w') as f:
        f.write(b'0123456789')

    assert hdfs.tail(a, 3) == b'789'
