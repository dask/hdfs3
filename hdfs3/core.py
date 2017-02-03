# -*- coding: utf-8 -*-
"Main module defining filesystem and file classes"
from __future__ import absolute_import

import ctypes
import logging
import os
import sys
import re
import warnings
from collections import deque
from .lib import _lib

PY3 = sys.version_info.major > 2

from .compatibility import FileNotFoundError, urlparse, ConnectionError
from .utils import read_block


logger = logging.getLogger(__name__)


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
        u = urlparse(conf['fs.defaultFS'])  # pragma: no cover
        conf['host'] = u.hostname  # pragma: no cover
        conf['port'] = u.port  # pragma: no cover
    return conf


def conf_to_dict(fname):
    """ Read a hdfs-site.xml style conf file, produces dictionary """
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
            conf[key] = val
    return conf


conf = hdfs_conf()
DEFAULT_HOST = conf.get('host', 'localhost')
DEFAULT_PORT = int(conf.get('port', 8020))


def ensure_bytes(s):
    """ Give strings that ctypes is guaranteed to handle """
    if PY3 and isinstance(s, bytes):
        return s
    if not PY3 and isinstance(s, str):
        return s
    if hasattr(s, 'encode'):
        return s.encode()
    if hasattr(s, 'tobytes'):
        return s.tobytes()
    if isinstance(s, bytearray):
        return bytes(s)
    if not PY3 and hasattr(s, 'tostring'):
        return s.tostring()
    if isinstance(s, dict):
        return {k: ensure_bytes(v) for k, v in s.items()}
    else:
        # Perhaps it works anyway - could raise here
        return s


def ensure_string(s):
    """ Ensure that the result is a string

    >>> ensure_string(b'123')
    '123'
    >>> ensure_string('123')
    '123'
    >>> ensure_string({'x': b'123'})
    {'x': '123'}
    """
    if isinstance(s, dict):
        return {k: ensure_string(v) for k, v in s.items()}
    if hasattr(s, 'decode'):
        return s.decode()
    else:
        return s


def ensure_trailing_slash(s, ensure=True):
    """ Ensure that string ends with a slash

    >>> ensure_trailing_slash('/user/directory')
    '/user/directory/'
    >>> ensure_trailing_slash('/user/directory/')
    '/user/directory/'
    >>> ensure_trailing_slash('/user/directory/', False)
    '/user/directory'
    """
    slash = '/' if isinstance(s, str) else b'/'
    if ensure and not s.endswith(slash):
        s += slash
    if not ensure and s.endswith(slash):
        s = s[:-1]
    return s


class HDFileSystem(object):
    """ Connection to an HDFS namenode

    >>> hdfs = HDFileSystem(host='127.0.0.1', port=8020)  # doctest: +SKIP
    """
    _first_pid = None

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, user=None,
                 ticket_cache=None, token=None, pars=None, connect=True):
        """
        Parameters
        ----------
        host : str (default from config files)
            namenode hostname or IP address, in case of HA mode it is name
            of the cluster that can be found in "fs.defaultFS" option.
        port : int (8020)
            namenode RPC port usually 8020, in HA mode port mast be None
        user, ticket_cache, token : str
            kerberos things
        pars : {str: str}
            other parameters for hadoop, that you can find in hdfs-site.xml,
            primary used for enabling secure mode or passing HA configuration.
        """
        self.host = host
        self.port = port
        self.user = user
        self._handle = None

        if ticket_cache and token:
            m = "It is not possible to use ticket_cache and token in same time"
            raise RuntimeError(m)

        self.ticket_cache = ticket_cache
        self.token = token
        self.pars = pars or {}
        if connect:
            self.connect()

    def __getstate__(self):
        d = self.__dict__.copy()
        del d['_handle']
        logger.debug("Serialize with state: %s", d)
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
            return

        if HDFileSystem._first_pid is None:
            HDFileSystem._first_pid = os.getpid()
        elif HDFileSystem._first_pid != os.getpid():
            warnings.warn("Attempting to re-use hdfs3 in child process %d, "
                          "but it was initialized in parent process %d. "
                          "Beware that hdfs3 is not fork-safe and this may "
                          "lead to bugs or crashes."
                          % (os.getpid(), HDFileSystem._first_pid),
                          RuntimeWarning, stacklevel=2)

        o = _lib.hdfsNewBuilder()
        if self.port is not None:
            _lib.hdfsBuilderSetNameNodePort(o, self.port)
        _lib.hdfsBuilderSetNameNode(o, ensure_bytes(self.host))
        if self.user:
            _lib.hdfsBuilderSetUserName(o, ensure_bytes(self.user))

        if self.ticket_cache:
            _lib.hdfsBuilderSetKerbTicketCachePath(o, ensure_bytes(self.ticket_cache))

        if self.token:
            _lib.hdfsBuilderSetToken(o, ensure_bytes(self.token))

        for par, val in self.pars.items():
            if not  _lib.hdfsBuilderConfSetStr(o, ensure_bytes(par), ensure_bytes(val)) == 0:
                warnings.warn('Setting conf parameter %s failed' % par)

        fs = _lib.hdfsBuilderConnect(o)
        _lib.hdfsFreeBuilder(o)
        if fs:
            logger.debug("Connect to handle %d", fs.contents.filesystem)
            self._handle = fs
            #if self.token:   # TODO: find out what a delegation token is
            #    self._token = _lib.hdfsGetDelegationToken(self._handle,
            #                                             ensure_bytes(self.user))
        else:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise ConnectionError('Connection Failed: {}'.format(msg))

    def disconnect(self):
        """ Disconnect from name node """
        if self._handle:
            logger.debug("Disconnect from handle %d", self._handle.contents.filesystem)
            _lib.hdfsDisconnect(self._handle)
        self._handle = None

    def open(self, path, mode='rb', replication=0, buff=0, block_size=0):
        """ Open a file for reading or writing

        Parameters
        ----------
        path: string
            Path of file on HDFS
        mode: string
            One of 'rb', 'wb', or 'ab'
        replication: int
            Replication factor; if zero, use system default (only on write)
        block_size: int
            Size of data-node blocks if writing
        """
        if not self._handle:
            raise IOError("Filesystem not connected")
        if block_size and mode != 'wb':
            raise ValueError('Block size only valid when writing new file')
        if ('a' in mode and self.exists(path) and
            replication !=0 and replication > 1):
            raise IOError("Appending to an existing file with replication > 1"
                    " is unsupported")
        if 'b' not in mode:
            raise NotImplementedError("Text mode not supported, use mode='%s'"
                    " and manage bytes" % (mode + 'b'))
        return HDFile(self, path, mode, replication=replication, buff=buff,
                block_size=block_size)

    def du(self, path, total=False, deep=False):
        """Returns file sizes on a path.

        Parameters
        ----------
        path : string
            where to look
        total : bool (False)
            to add up the sizes to a grand total
        deep : bool (False)
            whether to recurse into subdirectories
        """
        fi = self.ls(path)
        if deep:
            for apath in fi:
                if apath['kind'] == 'directory':
                    fi.extend(self.ls(apath['name']))
        if total:
            return {path: sum(f['size'] for f in fi)}
        return {p['name']: p['size'] for p in fi}

    def df(self):
        """ Used/free disc space on the HDFS system """
        cap = _lib.hdfsGetCapacity(self._handle)
        used = _lib.hdfsGetUsed(self._handle)
        return {'capacity': cap, 'used': used, 'percent-free': 100*(cap-used)/cap}

    def get_block_locations(self, path, start=0, length=0):
        """ Fetch physical locations of blocks """
        if not self._handle:
            raise IOError("Filesystem not connected")
        start = int(start) or 0
        length = int(length) or self.info(path)['size']
        nblocks = ctypes.c_int(0)
        out = _lib.hdfsGetFileBlockLocations(self._handle, ensure_bytes(path),
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
        """ File information (as a dict) """
        if not self.exists(path):
            raise FileNotFoundError(path)
        fi = _lib.hdfsGetPathInfo(self._handle, ensure_bytes(path)).contents
        out = info_to_dict(fi)
        _lib.hdfsFreeFileInfo(ctypes.byref(fi), 1)
        return ensure_string(out)

    def walk(self, path):
        """ Get all file entries below given path """
        return ([ensure_trailing_slash(ensure_string(path), False)]
                + list(self.du(path, False, True).keys()))

    def glob(self, path):
        """ Get list of paths mathing glob-like pattern (i.e., with "*"s).

        If passed a directory, gets all contained files; if passed path
        to a file, without any "*", returns one-element list containing that
        filename. Does not support python3.5's "**" notation.
        """
        path = ensure_string(path)
        try:
            f = self.info(path)
            if f['kind'] == 'directory' and '*' not in path:
                path = ensure_trailing_slash(path) + '*'
            else:
                return [f['name']]
        except IOError:
            pass
        if '/' in path[:path.index('*')]:
            ind = path[:path.index('*')].rindex('/')
            root = path[:ind+1]
        else:
            root = '/'
        allfiles = self.walk(root)
        pattern = re.compile("^" + path.replace('//', '/')
                                        .rstrip('/')
                                        .replace('*', '[^/]*')
                                        .replace('?', '.') + "$")
        out = [f for f in allfiles if re.match(pattern,
               f.replace('//', '/').rstrip('/'))]
        return out

    def ls(self, path, detail=True):
        """ List files at path

        Parameters
        ----------
        path : string/bytes
            location at which to list files
        detail : bool (=True)
            if True, each list item is a dict of file properties;
            otherwise, returns list of filenames
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        num = ctypes.c_int(0)
        fi = _lib.hdfsListDirectory(self._handle, ensure_bytes(path), ctypes.byref(num))
        out = [ensure_string(info_to_dict(fi[i])) for i in range(num.value)]
        _lib.hdfsFreeFileInfo(fi, num.value)
        if detail:
            return out
        else:
            return [o['name'] for o in out]

    def __repr__(self):
        if self._handle is None:
            state = 'Disconnected'
        else:
            state = 'Connected'
        return 'hdfs://%s:%s, %s' % (self.host, self.port, state)

    def __del__(self):
        if self._handle:
            self.disconnect()

    def mkdir(self, path):
        """ Make directory at path """
        out = _lib.hdfsCreateDirectory(self._handle, ensure_bytes(path))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('Create directory failed: {}'.format(msg))

    def set_replication(self, path, replication):
        """ Instruct HDFS to set the replication for the given file.

        If successful, the head-node's table is updated immediately, but
        actual copying will be queued for later. It is acceptable to set
        a replication that cannot be supported (e.g., higher than the
        number of data-nodes).
        """
        if replication < 0:
            raise ValueError('Replication must be positive, or 0 for system default')
        out = _lib.hdfsSetReplication(self._handle, ensure_bytes(path),
                                     ctypes.c_int16(int(replication)))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('Set replication failed: {}'.format(msg))

    def mv(self, path1, path2):
        """ Move file at path1 to path2 """
        if not self.exists(path1):
            raise FileNotFoundError(path1)
        out = _lib.hdfsRename(self._handle, ensure_bytes(path1), ensure_bytes(path2))
        return out == 0

    def rm(self, path, recursive=True):
        "Use recursive for `rm -r`, i.e., delete directory and contents"
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsDelete(self._handle, ensure_bytes(path), bool(recursive))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('Remove failed on %s %s' % (path, msg))

    def exists(self, path):
        """ Is there an entry at path? """
        out = _lib.hdfsExists(self._handle, ensure_bytes(path) )
        return out == 0

    def chmod(self, path, mode):
        """Change access control of given path

        Exactly what permissions the file will get depends on HDFS
        configurations.

        Parameters
        ----------
        path : string
            file/directory to change
        mode : integer
            As with the POSIX standard, each octal digit refers to
            user-group-all, in that order, with read-write-execute as the
            bits of each group.

        Examples
        --------
        >>> hdfs.chmod('/path/to/file', 0o777)  # make read/writeable to all # doctest: +SKIP
        >>> hdfs.chmod('/path/to/file', 0o700)  # make read/writeable only to user # doctest: +SKIP
        >>> hdfs.chmod('/path/to/file', 0o100)  # make read-only to user # doctest: +SKIP
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChmod(self._handle, ensure_bytes(path), ctypes.c_short(mode))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError("chmod failed on %s %s" % (path, msg))

    def chown(self, path, owner, group):
        """ Change owner/group """
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChown(self._handle, ensure_bytes(path), ensure_bytes(owner),
                            ensure_bytes(group))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError("chown failed on %s %s" % (path, msg))

    def cat(self, path):
        """ Return contents of file """
        if not self.exists(path):
            raise FileNotFoundError(path)
        with self.open(path, 'rb') as f:
            result = f.read()
        return result

    def get(self, hdfs_path, local_path, blocksize=2**16):
        """ Copy HDFS file to local """
        #TODO: _lib.hdfsCopy() may do this more efficiently
        if not self.exists(hdfs_path):
            raise FileNotFoundError(hdfs_path)
        with self.open(hdfs_path, 'rb') as f:
            with open(local_path, 'wb') as f2:
                out = 1
                while out:
                    out = f.read(blocksize)
                    f2.write(out)

    def getmerge(self, path, filename, blocksize=2**16):
        """ Concat all files in path (a directory) to output file """
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath['name'], 'rb') as f:
                    out = 1
                    while out:
                        out = f.read(blocksize)
                        f2.write(out)

    def put(self, filename, path, chunk=2**16, replication=0,block_size=0):
        """ Copy local file to path in HDFS """
        with self.open(path, 'wb',replication=replication,block_size=block_size) as f:
            with open(filename, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f.write(out)

    def tail(self, path, size=1024):
        """ Return last bytes of file """
        length = self.du(path)[ensure_trailing_slash(path)]
        if size > length:
            return self.cat(path)
        with self.open(path, 'rb') as f:
            f.seek(length - size)
            return f.read(size)

    def head(self, path, size=1024):
        """ Return first bytes of file """
        with self.open(path, 'rb') as f:
            return f.read(size)

    def touch(self, path):
        """ Create zero-length file """
        self.open(path, 'wb').close()

    def read_block(self, fn, offset, length, delimiter=None):
        """ Read a block of bytes from an HDFS file

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned will not include the surrounding delimiter strings.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename on HDFS
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> hdfs.read_block('/data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\\nBo'
        >>> hdfs.read_block('/data/file.csv', 0, 13, delimiter=b'\\n')  # doctest: +SKIP
        b'Alice, 100\\nBob, 200'

        See Also
        --------
        hdfs3.utils.read_block
        """
        with self.open(fn, 'rb') as f:
            size = f.info()['size']
            if offset + length > size:
                length = size - offset
            bytes = read_block(f, offset, length, delimiter)
        return bytes


def struct_to_dict(s):
    """ Return dictionary vies of a simple ctypes record-like structure """
    return dict((ensure_string(name), getattr(s, name)) for (name, p) in s._fields_)


def info_to_dict(s):
    """ Process data returned by hdfsInfo """
    d = struct_to_dict(s)
    d['kind'] = {68: 'directory', 70: 'file'}[d['kind']]
    return d


mode_numbers = {'w': 1, 'r': 0, 'a': 1025,
                'wb': 1, 'rb': 0, 'ab': 1025}

class HDFile(object):
    """ File on HDFS

    Matches the standard Python file interface.

    Examples
    --------
    >>> with hdfs.open('/path/to/hdfs/file.txt') as f:  # doctest: +SKIP
    ...     bytes = f.read(1000)  # doctest: +SKIP
    >>> with hdfs.open('/path/to/hdfs/file.csv') as f:  # doctest: +SKIP
    ...     df = pd.read_csv(f, nrows=1000)  # doctest: +SKIP
    """
    def __init__(self, fs, path, mode, replication=0, buff=0, block_size=0):
        """ Called by open on a HDFileSystem """
        if 't' in mode:
            raise NotImplementedError("Opening a file in text mode is not yet supported")
        self.fs = fs
        self.path = path
        self.replication = replication
        self.buff = buff
        self._fs = fs._handle
        self.buffers = []
        self._handle = None
        self.mode = mode
        self.block_size = block_size
        self.lines = deque([])
        self._set_handle()

    def _set_handle(self):
        out = _lib.hdfsOpenFile(self._fs, ensure_bytes(self.path),
                                mode_numbers[self.mode], self.buff,
                                ctypes.c_short(self.replication),
                                ctypes.c_int64(self.block_size))
        if not out:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError("Could not open file: %s, mode: %s %s" %
                          (self.path, self.mode, msg))
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

    def readline(self, chunksize=2**16, lineterminator='\n'):
        """ Return a line using buffered reading.

        Reads and caches chunksize bytes of data, and caches lines
        locally. Subsequent readline calls deplete those lines until
        empty, when a new chunk will be read. A read and readline are
        not therefore generally pointing to the same location in the file;
        `seek()` and `tell()` will give the true location in the file,
        which will be one chunk in even after calling `readline` once.

        Line iteration uses this method internally.
        """
        lineterminator = ensure_bytes(lineterminator)
        if len(self.lines) < 2:
            buffers = []
            while True:
                out = self.read(chunksize)
                buffers.append(out)
                if lineterminator in out:
                    break
                if not out:
                    remains = list(self.lines)
                    self.lines = deque([])
                    return b''.join(remains + buffers)
            self.lines = deque(b''.join(list(self.lines) +
                                        buffers).split(lineterminator))
        return self.lines.popleft() + lineterminator

    def _genline(self):
        while True:
            out = self.readline()
            if out:
                yield out
            else:
                raise StopIteration

    def __iter__(self):
        """ Enables `for line in file:` usage """
        return self._genline()

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def tell(self):
        """ Get current byte location in a file """
        out = _lib.hdfsTell(self._fs, self._handle)
        if out == -1:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('Tell Failed on file %s %s' % (self.path, msg))
        return out

    def seek(self, offset, from_what=0):
        """ Set file read position. Read mode only.

        Attempt to move out of file bounds raises an exception. Note that,
        by the convention in python file seek, offset should be <=0 if
        from_what is 2.

        Parameters
        ----------
        offset : int
            byte location in the file.
        from_what : int 0, 1, 2
            if 0 (befault), relative to file start; if 1, relative to current
            location; if 2, relative to file end.

        Returns
        -------
        new position
        """
        if from_what not in {0, 1, 2}:
            raise ValueError('seek mode must be 0, 1 or 2')
        info = self.info()
        if from_what == 1:
            offset = offset + self.tell()
        elif from_what == 2:
            offset = info['size'] + offset
        if offset < 0 or offset > info['size']:
            raise ValueError('Attempt to seek outside file')
        out = _lib.hdfsSeek(self._fs, self._handle, ctypes.c_int64(offset))
        if out == -1:
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('Seek Failed on file %s' % (self.path, msg))  # pragma: no cover
        return self.tell()

    def info(self):
        """ Filesystem metadata about this file """
        return self.fs.info(self.path)

    def write(self, data):
        """ Write bytes to open file (which must be in w or a mode) """
        data = ensure_bytes(data)
        if not data:
            return
        if not _lib.hdfsFileIsOpenForWrite(self._handle):
            msg = ensure_string(_lib.hdfsGetLastError())
            raise IOError('File not write mode: {}'.format(msg))
        write_block = 64 * 2**20
        for offset in range(0, len(data), write_block):
            d = ensure_bytes(data[offset:offset + write_block])
            if not _lib.hdfsWrite(self._fs, self._handle, d, len(d)) == len(d):
                msg = ensure_string(_lib.hdfsGetLastError())
                raise IOError('Write failed on file %s, %s' % (self.path, msg))
        return len(data)

    def flush(self):
        """ Send buffer to the data-node; actual write to disc may happen later """
        _lib.hdfsFlush(self._fs, self._handle)

    def close(self):
        """ Flush and close file, ensuring the data is readable """
        self.flush()
        _lib.hdfsCloseFile(self._fs, self._handle)
        self._handle = None  # _libhdfs releases memory
        self.mode = 'closed'

    @property
    def read1(self):
        return self.read

    @property
    def closed(self):
        return self.mode == 'closed'

    def writable(self):
        return self.mode.startswith('w') or self.mode.startswith('a')

    def seekable(self):
        return self.readable()

    def readable(self):
        return self.mode.startswith('r')

    def __del__(self):
        self.close()

    def __repr__(self):
        return 'hdfs://%s:%s%s, %s' % (self.fs.host, self.fs.port,
                                            self.path, self.mode)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
