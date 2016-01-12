# -*- coding: utf-8 -*-
"Main module defining filesystem and file classes"
from __future__ import absolute_import

import ctypes
import fnmatch
import os
import re
import subprocess
import sys
import warnings
try:
    from .lib import _lib
except (ImportError, OSError):
    print("Could not find libhdfs3.so")

PY3 = sys.version_info.major > 2

from .compatibility import FileNotFoundError, PermissionError, urlparse

def hdfs_conf():
    """ Load HDFS config from default locations. """
    confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                           '') + '/hadoop/conf')
    files = 'core-site.xml', 'hdfs-site.xml'
    conf = {}
    for afile in files:
        try:
            conf.update(conf_to_dict(os.sep.join([confd, afile])))
        except FileNotFoundError:
            pass
    if 'fs.defaultFS' in conf:
        u = urlparse(conf['fs.defaultFS'])
        conf['host'] = u.hostname
        conf['port'] = u.port
    return conf


def conf_to_dict(fname):
    name_match = re.compile("<name>(.*?)</name>")
    val_match = re.compile("<value>(.*?)</value>")
    conf = {}
    for line in open(fname):
        name = name_match.search(line)
        if name:
            key = name.groups()[0]
        val = val_match.search(line)
        if val:
            val = val.groups()[0]
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass
            if val == 'false':
                val = False
            if val == 'true':
                val = True
            conf[key] = val
    return conf

conf = hdfs_conf()

def ensure_byte(s):
    """ Give strings that ctypes is guaranteed to handle """
    if PY3:
        if isinstance(s, str):
            return s.encode('ascii')
        elif isinstance(s, bytes):
            return s
        else:
            raise ValueError('Could not convert %s to bytes' % s)
    else:  # in PY2, strings are fine for ctypes
        return s


def ensure_string(s):
    """ Ensure that the result is a string

    >>> ensure_string(b'123')
    u'123'
    """
    if hasattr(s, 'decode'):
        return s.decode()
    return s


def ensure_trailing_slash(s):
    """ Ensure that string ends with a slash

    >>> ensure_trailing_slash('/user/directory')
    '/user/directory/'
    >>> ensure_trailing_slash('/user/directory/')
    '/user/directory/'
    """
    if not s.endswith('/'):
        s += '/'
    return s


def init_kerb():
    """ Find Kerberos credentials.

    Use system kinit to find credentials.
    Set up by editing krb5.conf
    """
    raise NotImplementedError("Please set your credentials manually")
    out1 = subprocess.check_call(['kinit'])
    out2 = subprocess.check_call(['klist'])
    HDFileSystem.ticket_cache = None
    HDFileSystem.token = None


class HDFileSystem():
    """ Connection to an HDFS namenode

    >>> hdfs = HDFileSystem(host='127.0.0.1', port=50070)  # doctest: +SKIP
    """
    def __init__(self, host=None, port=None, user=None, ticket_cache=None,
            token=None, pars=None, connect=True):
        """
        Parameters
        ----------

        host : str (default from config files)
            namenode (name or IP)

        port : int (9000)
            connection port

        user, ticket_cache, token : str
            kerberos things

        pars : {str: str}
            other parameters for hadoop
        """
        self.host = host or conf.get('host', 'localhost')
        self.port = port or conf.get('port', 50070)
        self.user = user
        self.ticket_cache = ticket_cache
        self.pars = pars
        self.token = None  # Delegation token (generated)
        self._handle = None
        if connect:
            self.connect()

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['_handle']
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._handle = None
        self.connect()

    def connect(self):
        """ Connect to the name node

        This happens automatically at startup
        """
        if self._handle:
            raise ValueError("Already connected")

        o = _lib.hdfsNewBuilder()
        _lib.hdfsBuilderSetNameNodePort(o, self.port)
        _lib.hdfsBuilderSetNameNode(o, ensure_byte(self.host))
        if self.user:
            _lib.hdfsBuilderSetUserName(o, ensure_byte(self.user))
        if self.ticket_cache:
            _lib.hdfsBuilderSetKerbTicketCachePath(o, ensure_byte(self.ticket_cache))
        if self.token:
            _lib.hdfsBuilderSetToken(o, ensure_byte(self.token))
        if self.pars:
            for par in self.pars:
                if not  _lib.hdfsBuilderConfSetStr(o, ensure_byte(par),
                                          ensure_byte(self.pars(par))) == 0:
                    warnings.warn('Setting conf parameter %s failed' % par)
        fs = _lib.hdfsBuilderConnect(o)
        if fs:
            self._handle = fs
            #if self.token:   # TODO: find out what a delegation token is
            #    self._token = _lib.hdfsGetDelegationToken(self._handle,
            #                                             ensure_byte(self.user))
        else:
            raise RuntimeError('Connection Failed')

    def disconnect(self):
        """ Disconnect from name node """
        if self._handle:
            _lib.hdfsDisconnect(self._handle)
        self._handle = None

    def open(self, path, mode='r', repl=1, buff=0, block_size=0):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on HDFS
        mode: string
            One of 'r', 'w', or 'a'
        repl: int
            Replication factor
        block_size: int
            Size of data-node blocks if writing
        """
        if not self._handle:
            raise IOError("Filesystem not connected")
        if block_size and mode != 'w':
            raise ValueError('Block size only valid when writing new file')
        return HDFile(self, path, mode, repl=repl, buff=buff,
                block_size=block_size)

    def du(self, path, total=False, deep=False):
        if isinstance(total, str):
            total = total=='True'
        if isinstance(deep, str):
            deep = deep=='True'
        fi = self.ls(path)
        if len(fi) == 0:
            raise IOError('Not Found')
        if deep:
            for apath in fi:
                if apath['kind'] == 'directory':
                    fi.extend(self.ls(apath['name']))
        if total:
            return {path: sum(f['size'] for f in fi)}
        return {p['name']: p['size'] for p in fi}

    def df(self):
        cap = _lib.hdfsGetCapacity(self._handle)
        used = _lib.hdfsGetUsed(self._handle)
        return {'capacity': cap, 'used': used, 'free': 100*(cap-used)/cap}

    def get_block_locations(self, path, start=0, length=0):
        """ Fetch physical locations of blocks """
        if not self._handle:
            raise IOError("Filesystem not connected")
        start = int(start) or 0
        length = int(length) or self.info(path)['size']
        nblocks = ctypes.c_int(0)
        out = _lib.hdfsGetFileBlockLocations(self._handle, ensure_byte(path),
                                ctypes.c_int64(start), ctypes.c_int64(length),
                                ctypes.byref(nblocks))
        locs = []
        for i in range(nblocks.value):
            block = out[i]
            hosts = [block.hosts[i] for i in
                     range(block.numOfNodes)]
            locs.append({'hosts': hosts, 'length': block.length,
                         'offset': block.offset})
        _lib.hdfsFreeFileBlockLocations(out, nblocks)
        return locs

    def info(self, path):
        """ File information """
        if not self.exists(path):
            raise FileNotFoundError(path)
        fi = _lib.hdfsGetPathInfo(self._handle, ensure_byte(path)).contents
        out = info_to_dict(fi)
        _lib.hdfsFreeFileInfo(ctypes.byref(fi), 1)
        return out

    def glob(self, path):
        if "*" not in path:
            path = path + "*"
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.du(root, False, True).keys()
        out = [f for f in allfiles if fnmatch.fnmatch(ensure_string(f), path)]
        return out

    def ls(self, path):
        if not self.exists(path):
            raise FileNotFoundError(path)
        num = ctypes.c_int(0)
        fi = _lib.hdfsListDirectory(self._handle, ensure_byte(path), ctypes.byref(num))
        out = [info_to_dict(fi[i]) for i in range(num.value)]
        _lib.hdfsFreeFileInfo(fi, num.value)
        return out

    def __repr__(self):
        state = ['Disconnected', 'Connected'][self._handle is not None]
        return 'hdfs://%s:%s, %s' % (self.host, self.port, state)

    def __del__(self):
        if self._handle:
            self.disconnect()

    def mkdir(self, path):
        out = _lib.hdfsCreateDirectory(self._handle, ensure_byte(path))
        return out == 0

    def set_replication(self, path, repl):
        out = _lib.hdfsSetReplication(self._handle, ensure_byte(path),
                                     ctypes.c_int16(int(repl)))
        return out == 0

    def mv(self, path1, path2):
        if not self.exists(path1):
            raise FileNotFoundError(path1)
        out = _lib.hdfsRename(self._handle, ensure_byte(path1), ensure_byte(path2))
        return out == 0

    def rm(self, path, recursive=True):
        "Use recursive for `rm -r`, i.e., delete directory and contents"
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsDelete(self._handle, ensure_byte(path), bool(recursive))
        return out == 0

    def exists(self, path):
        out = _lib.hdfsExists(self._handle, ensure_byte(path) )
        return out == 0

    def truncate(self, path, pos):
        # Does not appear to ever succeed
        out = _lib.hdfsTruncate(self._handle, ensure_byte(path),
                               ctypes.c_int64(int(pos)), 0)
        return out == 0

    def chmod(self, path, mode):
        "Mode in numerical format (give as octal, if convenient)"
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChmod(self._handle, ensure_byte(path), ctypes.c_short(int(mode)))
        return out == 0

    def chown(self, path, owner, group):
        "Change owner/group"
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChown(self._handle, ensure_byte(path), ensure_byte(owner),
                            ensure_byte(group))
        return out == 0

    def cat(self, path):
        """ Return contents of file """
        if not self.exists(path):
            raise FileNotFoundError(path)
        buffers = []
        with self.open(path, 'r') as f:
            result = f.read()
        return result

    def get(self, path, filename):
        """ Copy HDFS file to local """
        #TODO: _lib.hdfsCopy() may do this more efficiently
        if not self.exists(path):
            raise FileNotFoundError(path)
        with self.open(path, 'r') as f:
            with open(filename, 'wb') as f2:
                out = 1
                while out:
                    out = f.read()
                    f2.write(out)

    def getmerge(self, path, filename):
        """ Concat all files in path (a directory) to output file """
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath['name'], 'r') as f:
                    out = 1
                    while out:
                        out = f.read()
                        f2.write(out)

    def put(self, filename, path, chunk=2**16):
        """ Copy local file to path in HDFS """
        #TODO: _lib.hdfsCopy() may do this more efficiently
        with self.open(path, 'w') as f:
            with open(filename, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f.write(out)

    def tail(self, path, size=None):
        """ Return last bytes of file """
        size = int(size) or 1024
        length = self.du(path)[ensure_trailing_slash(path)]
        if size > length:
            return self.cat(path)
        with self.open(path, 'r') as f:
            f.seek(length - size)
            return f.read(size)

    def touch(self, path):
        """ Create zero-length file """
        self.open(path, 'w').close()


def struct_to_dict(s):
    return dict((name, getattr(s, name)) for (name, p) in s._fields_)


def info_to_dict(s):
    d = struct_to_dict(s)
    d['kind'] = {68: 'directory', 70: 'file'}[d['kind']]
    return d


class HDFile(object):
    """ File on HDFS """
    def __init__(self, fs, path, mode, repl=1, buff=0, block_size=0):
        """ Called by open on a HDFileSystem """
        self.fs = fs
        self.path = path
        self.repl = repl
        self._fs = fs._handle
        self.buffer = b''
        self._handle = None
        self.encoding = 'ascii'
        m = {'w': 1, 'r': 0, 'a': 1025}[mode]
        self.mode = mode
        self.block_size = block_size
        out = _lib.hdfsOpenFile(self._fs, ensure_byte(path), m, buff,
                            ctypes.c_short(repl), ctypes.c_int64(block_size))
        if not out:
            raise IOError("Could not open file: %s, mode: %s" % (path, mode))
        self._handle = out

    def read(self, length=None):
        """ Read bytes from open file """
        if not _lib.hdfsFileIsOpenForRead(self._handle):
            raise IOError('File not read mode')
        buffers = []

        if length is None:
            out = 1
            while out:
                out = self.read(2**16)
                buffers.append(out)
        else:
            while length:
                bufsize = min(2**16, length)
                p = ctypes.create_string_buffer(bufsize)
                ret = _lib.hdfsRead(self._fs, self._handle, p, ctypes.c_int32(bufsize))
                if ret == 0:
                    break
                if ret > 0:
                    if ret < bufsize:
                        buffers.append(p.raw[:ret])
                    elif ret == bufsize:
                        buffers.append(p.raw)
                    length -= ret
                else:
                    raise IOError('Read file %s Failed:' % self.path, -ret)

        return b''.join(buffers)

    def readline(self):
        """ Read a buffered line, text mode. """
        lines = getattr(self, 'lines', [])
        if len(lines) < 1:
            buff = self.read()
            if len(buff) == 0:   #EOF
                remains = self.buffer.decode(self.encoding)
                if remains:
                    self.buffer = b''
                    return remains
                raise EOFError
            buff = (self.buffer + buff).decode(self.encoding)
            self.lines = buff.split('\n')
        return self.lines.pop(0)

    def _genline(self):
        while True:
            try:
                yield self.readline()
            except EOFError:
                raise StopIteration

    def __iter__(self):
        return self._genline()

    def readlines(self):
        return list(self)

    def tell(self):
        out = _lib.hdfsTell(self._fs, self._handle)
        if out == -1:
            raise IOError('Tell Failed on file %s' % self.path)
        return out

    def seek(self, loc):
        out = _lib.hdfsSeek(self._fs, self._handle, ctypes.c_int64(loc))
        if out == -1:
            raise IOError('Seek Failed on file %s' % self.path)

    def info(self):
        """ Filesystem metadata about this file """
        return self.fs.info(self.path)

    def write(self, data):
        data = ensure_byte(data)
        if not _lib.hdfsFileIsOpenForWrite(self._handle):
            raise IOError('File not write mode')
        if not _lib.hdfsWrite(self._fs, self._handle, data, len(data)) == len(data):
            raise IOError('Write failed on file %s' % self.path)

    def flush(self):
        _lib.hdfsFlush(self._fs, self._handle)

    def close(self):
        self.flush()
        _lib.hdfsCloseFile(self._fs, self._handle)
        self._handle = None  # _libhdfs releases memory
        self.mode = 'closed'

    def get_block_locs(self):
        """
        Get host locations and offsets of all blocks that compose this file
        """
        return self.fs.get_block_locations(self.path)

    def __del__(self):
        self.close()

    def __repr__(self):
        return 'hdfs://%s:%s%s, %s' % (self.fs.host, self.fs.port,
                                            self.path, self.mode)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
