from __future__ import unicode_literals

import io
import multiprocessing
import os
import tempfile
import sys
from random import randint
try:
    from queue import Queue
except ImportError:
    from Queue import Queue
from threading import Thread
import traceback

import pytest

from hdfs3 import HDFileSystem, lib
from hdfs3.core import conf_to_dict, ensure_bytes, ensure_string
from hdfs3.core import DEFAULT_HOST, DEFAULT_PORT
from hdfs3.compatibility import bytes, unicode, ConnectionError
from hdfs3.utils import tmpfile


@pytest.yield_fixture
def hdfs():
    hdfs = HDFileSystem(host='localhost', port=8020,
                        pars={'rpc.client.connect.retry': '2'})
    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.mkdir('/tmp/test')

    yield hdfs

    if hdfs.exists('/tmp/test'):
        hdfs.rm('/tmp/test')
    hdfs.disconnect()


a = '/tmp/test/a'
b = '/tmp/test/b'
c = '/tmp/test/c'
d = '/tmp/test/d'


def test_simple(hdfs):
    data = b'a' * (10 * 2**20)

    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(data)

    with hdfs.open(a, 'rb') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data


def test_default_port_and_host():
    hdfs = HDFileSystem(connect=False)
    assert hdfs.host == DEFAULT_HOST
    assert hdfs.port == DEFAULT_PORT


def test_token_and_ticket_cache_in_same_time():
    ticket_cache = "/tmp/krb5cc_0"
    token = "abc"

    with pytest.raises(RuntimeError) as ctx:
        HDFileSystem(connect=False, ticket_cache=ticket_cache, token=token)

    msg = "It is not possible to use ticket_cache and token in same time"
    assert msg in str(ctx.value)


@pytest.mark.slow
def test_connection_error():
    with pytest.raises(ConnectionError) as ctx:
        hdfs = HDFileSystem(host='localhost', port=9999, connect=False,
                            pars={'rpc.client.connect.retry': '1'})
        hdfs.CONNECT_RETRIES = 1
        hdfs.connect()
    # error message is long and with java exceptions, so here we just check
    # that important part of error is present
    msg = 'Caused by: HdfsNetworkConnectException: Connect to "localhost:9999"'
    assert msg in str(ctx.value)

def test_idempotent_connect(hdfs):
    hdfs.connect()
    hdfs.connect()


def test_ls_touch(hdfs):
    assert not hdfs.ls('/tmp/test')
    hdfs.touch(a)
    hdfs.touch(b)
    L = hdfs.ls('/tmp/test')
    assert set(d['name'] for d in L) == set([a, b])
    L = hdfs.ls('/tmp/test', False)
    assert set(L) == set([a, b])


def test_rm(hdfs):
    assert not hdfs.exists(a)
    hdfs.touch(a)
    assert hdfs.exists(a)
    hdfs.rm(a)
    assert not hdfs.exists(a)


def test_pickle(hdfs):
    data = b'a' * (10 * 2**20)
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(data)

    assert hdfs._handle
    import pickle
    hdfs2 = pickle.loads(pickle.dumps(hdfs))
    assert hdfs2._handle

    hdfs2.touch(b)
    hdfs2.ls(b)

    with hdfs2.open(c, 'wb', replication=1) as f:
        f.write(data)
        assert f._handle

    with hdfs2.open(c, 'rb') as f:
        f.seek(5)
        f.read(10)
        assert f._handle

    with hdfs.open(d, 'wb', replication=1) as f:
        f.write(data)
        assert f._handle


def test_seek(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123')

    with hdfs.open(a) as f:
        with pytest.raises(ValueError):
            f.seek(1000)
        with pytest.raises(ValueError):
            f.seek(-1)
        with pytest.raises(ValueError):
            f.seek(-5, 2)
        with pytest.raises(ValueError):
            f.seek(0, 10)
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(0)
        assert f.read(1) == b'1'
        f.seek(3)
        assert f.read(1) == b''
        f.seek(-1, 2)
        assert f.read(1) == b'3'
        f.seek(-1, 1)
        f.seek(-1, 1)
        assert f.read(1) == b'2'
        for i in range(4):
            assert f.seek(i) == i



def test_libload():
    assert lib.hdfsGetLastError()
    assert len(lib.hdfsGetLastError.__doc__) > 0
    assert lib.hdfsFileIsOpenForRead(lib.hdfsFile()) == False


def test_bad_open(hdfs):
    with pytest.raises(IOError):
        hdfs.open('')


@pytest.mark.xfail
def test_write_blocksize(hdfs):
    with hdfs.open(a, 'wb', block_size=10) as f:
        f.write(b'a' * 25)

    blocks = hdfs.get_block_locations(a)
    assert len(blocks) == 3
    assert blocks[0]['length'] == 10
    assert blocks[1]['length'] == 10
    assert blocks[2]['length'] == 5

    with pytest.raises(ValueError):
        hdfs.open(a, 'rb', block_size=123)


@pytest.mark.slow
def test_write_vbig(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b' ' * 2**31)
    assert hdfs.info(a)['size'] == 2**31
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b' ' * (2**31 + 1))
    assert hdfs.info(a)['size'] == 2**31 + 1


def test_replication(hdfs):
    path = '/tmp/test/afile'
    hdfs.open(path, 'wb', replication=0).close()
    assert hdfs.info(path)['replication'] > 0
    hdfs.open(path, 'wb', replication=1).close()
    assert hdfs.info(path)['replication'] == 1
    hdfs.open(path, 'wb', replication=2).close()
    assert hdfs.info(path)['replication'] == 2
    hdfs.set_replication(path, 3)
    assert hdfs.info(path)['replication'] == 3
    with pytest.raises(ValueError):
        hdfs.set_replication(path, -1)
    with pytest.raises(IOError):
        hdfs.open(path, 'wb', replication=-1).close()

def test_errors(hdfs):
    with pytest.raises((IOError, OSError)):
        hdfs.open('/tmp/test/shfoshf', 'rb')

    with pytest.raises((IOError, OSError)):
        hdfs.touch('/tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        hdfs.rm('/tmp/test/shfoshf/x')

    with pytest.raises((IOError, OSError)):
        hdfs.mv('/tmp/test/shfoshf/x', '/tmp/test/shfoshf/y')

    with pytest.raises((IOError, OSError)):
        hdfs.open('/nonexistent/x', 'wb')

    with pytest.raises((IOError, OSError)):
        hdfs.open('/nonexistent/x', 'rb')

    with pytest.raises(IOError):
        hdfs.chown('/unknown', 'someone', 'group')

    with pytest.raises(IOError):
        hdfs.chmod('/unknonwn', 'rb')

    with pytest.raises(IOError):
        hdfs.rm('/unknown')


def test_glob_walk(hdfs):
    hdfs.mkdir('/tmp/test/c/')
    hdfs.mkdir('/tmp/test/c/d/')
    filenames = ['a', 'a1', 'a2', 'a3', 'b1', 'c/x1', 'c/x2', 'c/d/x3']
    filenames = ['/tmp/test/' + s for s in filenames]
    for fn in filenames:
        hdfs.touch(fn)

    assert set(hdfs.glob('/tmp/test/a*')) == {'/tmp/test/a',
                                              '/tmp/test/a1',
                                              '/tmp/test/a2',
                                              '/tmp/test/a3'}

    assert set(hdfs.glob('/tmp/test/c/*')) == {'/tmp/test/c/x1',
                                               '/tmp/test/c/x2',
                                               '/tmp/test/c/d'}
    assert (set(hdfs.glob('/tmp/test/c')) ==
            set(hdfs.glob('/tmp/test/c/')) ==
            set(hdfs.glob('/tmp/test/c/*')))

    assert set(hdfs.glob('/tmp/test/a')) == {'/tmp/test/a'}
    assert set(hdfs.glob('/tmp/test/a1')) == {'/tmp/test/a1'}

    assert set(hdfs.glob('/tmp/test/*')) == {'/tmp/test/a',
                                             '/tmp/test/a1',
                                             '/tmp/test/a2',
                                             '/tmp/test/a3',
                                             '/tmp/test/b1',
                                             '/tmp/test/c'}

    assert set(hdfs.walk('/tmp/test')) == {'/tmp/test',
                                           '/tmp/test/a',
                                           '/tmp/test/a1',
                                           '/tmp/test/a2',
                                           '/tmp/test/a3',
                                           '/tmp/test/b1',
                                           '/tmp/test/c',
                                           '/tmp/test/c/x1',
                                           '/tmp/test/c/x2',
                                           '/tmp/test/c/d',
                                           '/tmp/test/c/d/x3'}

    assert set(hdfs.walk('/tmp/test/c/')) == {'/tmp/test/c',
                                              '/tmp/test/c/x1',
                                              '/tmp/test/c/x2',
                                              '/tmp/test/c/d',
                                              '/tmp/test/c/d/x3'}

    assert set(hdfs.walk('/tmp/test/c/')) == set(hdfs.walk('/tmp/test/c'))


def test_info(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write('a' * 5)

    info = hdfs.info(a)
    assert info['size'] == 5
    assert info['name'] == a
    assert info['kind'] == 'file'
    assert info['replication'] == 1

    assert hdfs.info('/')['kind'] == 'directory'


def test_df(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write('a' * 10)
    with hdfs.open(b, 'wb', replication=1) as f:
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
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'0123456789')
    assert hdfs.cat(a) == b'0123456789'
    with pytest.raises(IOError):
        hdfs.cat(b)

def test_full_read(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'0123456789')

    with hdfs.open(a, 'rb') as f:
        assert len(f.read(4)) == 4
        assert len(f.read(4)) == 4
        assert len(f.read(4)) == 2

    with hdfs.open(a, 'rb') as f:
        assert len(f.read()) == 10

    with hdfs.open(a, 'rb') as f:
        assert f.tell() == 0
        f.seek(3)
        assert f.read(4) == b'3456'
        assert f.tell() == 7
        assert f.read(4) == b'789'
        assert f.tell() == 10

def test_tail_head(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'0123456789')

    assert hdfs.tail(a, 3) == b'789'
    assert hdfs.head(a, 3) == b'012'
    assert hdfs.tail(a, 100) == b'0123456789'


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
    should = {'dfs.block.size': '134217728',
             'dfs.datanode.hdfs-blocks-metadata.enabled': 'true',
             'dfs.namenode.name.dir': '/mnt/data/dfs/nn',
             'dfs.permissions': 'false',
             'dfs.permissions.superusergroup': 'hadoop',
             'dfs.replication': '3'}
    assert conf_to_dict(conffile) == should


def test_read_delimited_block(hdfs):
    fn = '/tmp/test/a'
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])

    with hdfs.open(fn, 'wb', replication=1) as f:
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
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(lineterminator.join([b'123', b'456', b'789']))

    with hdfs.open(a) as f:
        assert f.readline(lineterminator=lineterminator) == b'123'+lineterminator
        assert f.readline(lineterminator=lineterminator) == b'456'+lineterminator
        assert f.readline(lineterminator=lineterminator) == b'789'
        assert f.readline(lineterminator=lineterminator) == b''


def read_write(hdfs, q, i):
    try:
        hdfs.df()
        hdfs.du('/', True, True)
        data = b'0' * 10000
        fn = '/tmp/test/foo%d' % i
        with hdfs.open(fn, 'wb', replication=1) as f:
            f.write(data)
        with hdfs.open(fn, 'rb') as f:
            data2 = f.read()
        assert data == data2
    except BaseException as e:
        traceback.print_exc()
        q.put(str(e))
    else:
        q.put(None)


@pytest.mark.skipif(sys.version_info < (3, 4), reason='No spawn')
def test_stress_embarrassing(hdfs):
    if sys.version_info < (3, 4):
        return

    ctx = multiprocessing.get_context('spawn')
    for proc, queue in [(Thread, Queue), (ctx.Process, ctx.Queue)]:
        q = queue()
        threads = [proc(target=read_write, args=(hdfs, q, i)) for
                   i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        while not q.empty():
            error = q.get()
            if error:
                raise AssertionError("error in child: " + error)


def read_random_block(hdfs, fn, n, delim):
    for i in range(10):
        hdfs.read_block(fn, randint(0, n/2), randint(n/2, n), delim)


@pytest.mark.skipif(sys.version_info < (3, 4), reason='No spawn')
def test_stress_read_block(hdfs):
    ctx = multiprocessing.get_context('spawn')
    data = b'hello, world!\n' * 10000

    for T in (Thread, ctx.Process,):
        with hdfs.open(a, 'wb', replication=1) as f:
            f.write(data)

        threads = [T(target=read_random_block, args=(hdfs, a, len(data), b'\n'))
                    for i in range(4)]
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()


def test_different_handles():
    a = HDFileSystem(host='localhost', port=8020)
    b = HDFileSystem(host='localhost', port=8020)
    assert a._handle.contents.filesystem != b._handle.contents.filesystem


def handle(q):
    hdfs = HDFileSystem(host='localhost', port=8020)
    q.put(hdfs._handle.contents.filesystem)


@pytest.mark.skipif(sys.version_info < (3, 4), reason='No spawn')
def test_different_handles_in_processes():
    ctx = multiprocessing.get_context('spawn')

    hdfs = HDFileSystem(host='localhost', port=8020)
    q = ctx.Queue()
    n = 20
    procs = [ctx.Process(target=handle, args=(q,)) for i in range(n)]
    for p in procs:
        p.daemon = True
        p.start()

    s = {q.get() for i in range(n)}
    assert not any(i == hdfs._handle.contents.filesystem for i in s)

    for p in procs:
        p.join()


@pytest.mark.skipif(not hasattr(os, 'fork'), reason='No fork()')
def test_warn_on_fork():
    hdfs = HDFileSystem(host='localhost', port=8020)
    hdfs.disconnect()

    pid = os.fork()
    if not pid:
        # In child
        try:
            with pytest.warns(RuntimeWarning) as record:
                hdfs = HDFileSystem(host='localhost', port=8020)
            assert len(record) == 1
            assert ("Attempting to re-use hdfs3 in child process"
                    in str(record[0].message))
        except BaseException:
            print("\n------ Child exception -------")
            traceback.print_exc()
            print("------------------------------")
            os._exit(1)
        else:
            os._exit(0)
    retpid, status = os.waitpid(pid, 0)
    assert retpid == pid
    if status:
        pytest.fail("child raised exception")


def test_ensure():
    assert isinstance(ensure_bytes(''), bytes)
    assert isinstance(ensure_bytes(b''), bytes)
    assert isinstance(ensure_string(''), unicode)
    assert isinstance(ensure_string(b''), unicode)
    assert ensure_string({'x': b'', 'y': ''}) == {'x': '', 'y': ''}
    assert ensure_bytes({'x': b'', 'y': ''}) == {'x': b'', 'y': b''}



def test_touch_exists(hdfs):
    hdfs.touch(a)
    assert hdfs.exists(a)


def test_write_in_read_mode(hdfs):
    hdfs.touch(a)

    with hdfs.open(a, 'rb') as f:
        with pytest.raises(IOError):
            f.write(b'123')


def test_readlines(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123\n456')

    with hdfs.open(a, 'rb') as f:
        lines = f.readlines()
        assert lines == [b'123\n', b'456']

    with hdfs.open(a, 'rb') as f:
        assert list(f) == lines

    with hdfs.open(a, 'wb', replication=1) as f:
        with pytest.raises(IOError):
            f.read()

    bigdata = [b'fe', b'fi', b'fo'] * 32000
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'\n'.join(bigdata))
    with hdfs.open(a, 'rb') as f:
        lines = list(f)
    assert all(l in [b'fe\n', b'fi\n', b'fo', b'fo\n'] for l in lines)


def test_put(hdfs):
    data = b'1234567890' * 10000
    with tmpfile() as fn:
        with open(fn, 'wb') as f:
            f.write(data)

        hdfs.put(fn, a)

        assert hdfs.cat(a) == data


def test_getmerge(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123')
    with hdfs.open(b, 'wb', replication=1) as f:
        f.write(b'456')

    with tmpfile() as fn:
        hdfs.getmerge('/tmp/test', fn)

        with open(fn, 'rb') as f:
            data = f.read()
        assert data == b'123456'


def test_get(hdfs):
    data = b'1234567890'
    with tmpfile() as fn:
        with hdfs.open(a, 'wb', replication=1) as f:
            f.write(data)

        hdfs.get(a, fn)

        with open(fn, 'rb') as f:
            data2 = f.read()
        assert data2 == data

    with pytest.raises(IOError):
        hdfs.get(b, fn)


def test_open_errors(hdfs):
    hdfs.touch(a)
    with pytest.raises(ValueError):
        hdfs.open(a, 'rb', block_size=1000)

    hdfs.disconnect()
    with pytest.raises(IOError):
        hdfs.open(a, 'wb', replication=1)


def test_du(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123')
    with hdfs.open(b, 'wb', replication=1) as f:
        f.write(b'4567')

    assert hdfs.du('/tmp/test') == {a: 3, b: 4}
    assert hdfs.du('/tmp/test/', total=True) == {'/tmp/test/': 3 + 4}


def test_get_block_locations(hdfs):
    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123')

    locs = hdfs.get_block_locations(a)
    assert len(locs) == 1
    assert locs[0]['length'] == 3


def test_chmod(hdfs):
    hdfs.touch(a)
    assert hdfs.ls(a)[0]['permissions'] == 0o777
    hdfs.chmod(a, 0o500)
    assert hdfs.ls(a)[0]['permissions'] == 0o500
    hdfs.chmod(a, 0o100)
    with pytest.raises(IOError):
        hdfs.open(a, 'ab')


@pytest.mark.xfail
def test_chown(hdfs):
    hdfs.touch(a)
    hdfs.info(a)
    hdfs.chown(a, 'root', 'supergroup')


def test_text_bytes(hdfs):
    with pytest.raises(NotImplementedError):
        hdfs.open(a, 'wt')

    with pytest.raises(NotImplementedError):
        hdfs.open(a, 'rt')

    try:
        hdfs.open(a, 'r')
    except NotImplementedError as e:
        assert 'rb' in str(e)

    try:
        hdfs.open(a, 'w')
    except NotImplementedError as e:
        assert 'wb' in str(e)

    try:
        hdfs.open(a, 'a')
    except NotImplementedError as e:
        assert 'ab' in str(e)

    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(b'123')

    with hdfs.open(a, 'rb') as f:
        b = f.read()

    assert b == b'123'


def test_open_deep_file(hdfs):
    with pytest.raises(IOError) as ctx:
        hdfs.open('/tmp/test/a/b/c/d/e/f', 'wb')
    msg = "Could not open file: /tmp/test/a/b/c/d/e/f, mode: wb " \
          "Parent directory doesn't exist"
    assert msg in str(ctx.value)

def test_append(hdfs):
    with hdfs.open(a, mode='ab', replication=1) as f:
        f.write(b'123')
    with hdfs.open(a, mode='ab', replication=1) as f:
        f.write(b'456')

    with hdfs.open(a, mode='rb') as f:
        assert f.read() == b'123456'

    with hdfs.open(a, mode='ab', replication=1) as f:
        f.write(b'789')
    with hdfs.open(a, mode='rb') as f:
        assert f.read() == b'123456789'

    with pytest.raises(IOError):
        with hdfs.open(b, mode='ab', replication=2) as f:
            f.write(b'123')
        with hdfs.open(b, mode='ab', replication=2) as f:
            f.write(b'456')


def test_write_empty(hdfs):
    with hdfs.open(a, mode='wb', replication=1) as f:
        f.write(b'')

    with hdfs.open(a, mode='rb') as f:
        assert f.read() == b''


def test_gzip(hdfs):
    import gzip
    data = b'name,amount\nAlice,100\nBob,200'
    with hdfs.open(a, mode='wb', replication=1) as f:
        with gzip.GzipFile(fileobj=f) as g:
            g.write(b'name,amount\nAlice,100\nBob,200')

    with hdfs.open(a) as f:
        with gzip.GzipFile(fileobj=f) as g:
            bytes = g.read()

    assert bytes == data


def test_fooable(hdfs):
    hdfs.touch(a)

    with hdfs.open(a, mode='rb', replication=1) as f:
        assert f.readable()
        assert f.seekable()
        assert not f.writable()

    with hdfs.open(a, mode='wb', replication=1) as f:
        assert not f.readable()
        assert not f.seekable()
        assert f.writable()


def test_closed(hdfs):
    hdfs.touch(a)

    f = hdfs.open(a, mode='rb')
    assert not f.closed
    f.close()
    assert f.closed


def test_TextIOWrapper(hdfs):
    with hdfs.open(a, mode='wb', replication=1) as f:
        f.write(b'1,2\n3,4\n5,6')

    with hdfs.open(a, mode='rb') as f:
        ff = io.TextIOWrapper(f)
        data = list(ff)

    assert data == ['1,2\n', '3,4\n', '5,6']


def test_array(hdfs):
    from array import array
    data = array('B', [65] * 1000)

    with hdfs.open(a, 'wb', replication=1) as f:
        f.write(data)

    with hdfs.open(a, 'rb') as f:
        out = f.read()
        assert out == b'A' * 1000
