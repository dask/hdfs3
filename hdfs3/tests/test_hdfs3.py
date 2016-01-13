from hdfs3 import HDFileSystem, lib
from hdfs3.core import conf_to_dict
from hdfs3.compatibility import FileNotFoundError, PermissionError
import pytest
import ctypes
import os
import tempfile

@pytest.yield_fixture
def hdfs():
    hdfs = HDFileSystem(host='localhost', port=8020)
    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    yield hdfs

    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')


a = '/tmp/test/a'
b = '/tmp/test/b'
c = '/tmp/test/c'
d = '/tmp/test/d'


def test_example(hdfs):
    data = b'a' * (10 * 2**20)

    with hdfs.open(a, 'w', repl=1) as f:
        f.write(data)

    with hdfs.open(a, 'r') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_ls_touch(hdfs):
    assert not hdfs.ls('/tmp/test')
    hdfs.touch(a)
    hdfs.touch(b)
    L = hdfs.ls('/tmp/test')
    assert set(d['name'] for d in L) == set([a, b])


def test_rm(hdfs):
    assert not hdfs.exists(a)
    hdfs.touch(a)
    assert hdfs.exists(a)
    hdfs.rm(a)
    assert not hdfs.exists(a)


def test_pickle(hdfs):
    data = b'a' * (10 * 2**20)
    with hdfs.open(a, 'w', repl=1) as f:
        f.write(data)

    assert hdfs._handle > 0
    import pickle
    hdfs2 = pickle.loads(pickle.dumps(hdfs))
    assert hdfs2._handle > 0

    hdfs2.touch(b)
    hdfs2.ls(b)

    with hdfs2.open(c, 'w', repl=1) as f:
        f.write(data)
        assert f._handle

    with hdfs2.open(c, 'r') as f:
        f.seek(5)
        f.read(10)
        assert f._handle

    with hdfs.open(d, 'w', repl=1) as f:
        f.write(data)
        assert f._handle


def test_seek(hdfs):
    with hdfs.open(a, 'w', repl=1) as f:
        f.write(b'123')

    with hdfs.open(a) as f:
        f.seek(1000)
        assert not f.read(1)
        f.seek(0)
        assert f.read(1) == b'1'


def test_libload():
    assert lib.hdfsGetLastError()
    assert len(lib.hdfsGetLastError.__doc__) > 0
    assert lib.hdfsFileIsOpenForRead(lib.hdfsFile()) == False


def test_bad_open(hdfs):
    with pytest.raises(IOError):
        hdfs.open('')


@pytest.mark.xfail
def test_write_blocksize(hdfs):
    with hdfs.open(a, 'w', block_size=10) as f:
        f.write(b'a' * 25)

    blocks = hdfs.get_block_locations(a)
    assert len(blocks) == 3
    assert blocks[0]['length'] == 10
    assert blocks[1]['length'] == 10
    assert blocks[2]['length'] == 5

    with pytest.raises(ValueError):
        hdfs.open(a, 'r', block_size=123)


def test_errors(hdfs):
    with pytest.raises(FileNotFoundError):
        hdfs.open('/tmp/test/shfoshf', 'r')

    with pytest.raises(FileNotFoundError):
        hdfs.touch('/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        hdfs.rm('/tmp/test/shfoshf/x')

    with pytest.raises(FileNotFoundError):
        hdfs.mv('/tmp/test/shfoshf/x', '/tmp/test/shfoshf/y')

    with pytest.raises(PermissionError):
        hdfs.open('/x', 'w')

    with pytest.raises(PermissionError):
        hdfs.open('/x', 'r')


def test_glob(hdfs):
    hdfs.mkdir('/tmp/test/c/')
    hdfs.mkdir('/tmp/test/c/d/')
    filenames = ['a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
    filenames = ['/tmp/test/' + s for s in filenames]
    for fn in filenames:
        hdfs.touch(fn)

    assert set(hdfs.glob('/tmp/test/a*')) == set(['/tmp/test/' + a
                                              for a in ['a1', 'a2', 'a3']])
    assert len(hdfs.glob('/tmp/test/c/')) == 4
    assert set(hdfs.glob('/tmp/test/')).issuperset(filenames)


def test_info(hdfs):
    with hdfs.open(a, 'w', repl=1) as f:
        f.write('a' * 5)

    info = hdfs.info(a)
    assert info['size'] == 5
    assert info['name'] == a
    assert info['kind'] == 'file'
    assert info['replication'] == 1

    assert hdfs.info('/')['kind'] == 'directory'


def test_df(hdfs):
    with hdfs.open(a, 'w', repl=1) as f:
        f.write('a' * 10)
    with hdfs.open(b, 'w', repl=1) as f:
        f.write('a' * 10)

    result = hdfs.df()
    assert result['capacity'] > result['used']


def test_move(hdfs):
    hdfs.touch(a)
    assert hdfs.exists(a)
    assert not hdfs.exists(b)
    hdfs.mv(a, b)
    assert not hdfs.exists(a)
    assert hdfs.exists(b)


@pytest.mark.xfail
def test_copy(hdfs):
    hdfs.touch(a)
    assert hdfs.exists(a)
    assert not hdfs.exists(b)
    hdfs.cp(a, b)
    assert hdfs.exists(a)
    assert hdfs.exists(b)


def test_exists(hdfs):
    assert not hdfs.exists(a)
    hdfs.touch(a)
    assert hdfs.exists(a)
    hdfs.rm(a)
    assert not hdfs.exists(a)


def test_cat(hdfs):
    with hdfs.open(a, 'w') as f:
        f.write(b'0123456789')
    assert hdfs.cat(a) == b'0123456789'


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

@pytest.yield_fixture
def conffile():
    fd, fname = tempfile.mkstemp()
    open(fname, 'w').write("""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
  <property>
    <name>dfs.permissions.superusergroup</name>
    <value>hadoop</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/mnt/data/dfs/nn</value>
  </property>

  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>

  <property>
    <name>dfs.block.size</name>
    <value>134217728</value>
  </property>

  <property>
    <name>dfs.datanode.hdfs-blocks-metadata.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>dfs.permissions</name>
    <value>false</value>
  </property>
</configuration>
""")
    yield fname
    if os.path.exists(fname):
        os.unlink(fname)


def test_conf(conffile):
    should = {'dfs.block.size': 134217728,
             'dfs.datanode.hdfs-blocks-metadata.enabled': True,
             'dfs.namenode.name.dir': '/mnt/data/dfs/nn',
             'dfs.permissions': False,
             'dfs.permissions.superusergroup': 'hadoop',
             'dfs.replication': 3}
    assert conf_to_dict(conffile) == should


def test_read_delimited_block(hdfs):
    fn = '/tmp/test/a'
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])

    with hdfs.open(fn, 'w') as f:
        f.write(data)

    assert hdfs.read_block(fn, 1, 2) == b'23'
    assert hdfs.read_block(fn, 0, 1, delimiter=b'\n') == b'123'
    assert hdfs.read_block(fn, 0, 2, delimiter=b'\n') == b'123'
    assert hdfs.read_block(fn, 0, 3, delimiter=b'\n') == b'123'
    assert hdfs.read_block(fn, 0, 5, delimiter=b'\n') == b'123\n456'
    assert hdfs.read_block(fn, 0, 8, delimiter=b'\n') == b'123\n456\n789'
    assert hdfs.read_block(fn, 0, 100, delimiter=b'\n') == b'123\n456\n789'
    assert hdfs.read_block(fn, 1, 1, delimiter=b'\n') == b''
    assert hdfs.read_block(fn, 1, 5, delimiter=b'\n') == b'456'
    assert hdfs.read_block(fn, 1, 8, delimiter=b'\n') == b'456\n789'

    for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                [(0, 4), (4, 4), (8, 4)]]:
        out = [hdfs.read_block(fn, o, l, b'\n') for o, l in ols]
        assert delimiter.join(filter(None, out)) == data


@pytest.mark.parametrize(['lineterminator'], [(b'\n',), (b'--',)])
def test_readline(hdfs, lineterminator):
    with hdfs.open(a, 'w') as f:
        f.write(lineterminator.join([b'123', b'456', b'789']))

    with hdfs.open(a) as f:
        assert f.readline(lineterminator=lineterminator) == '123'
        assert f.readline(lineterminator=lineterminator) == '456'
        assert f.readline(lineterminator=lineterminator) == '789'
        with pytest.raises(EOFError):
            f.readline()
