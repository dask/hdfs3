# -*- coding: utf-8 -*-
"Main module defining filesystem and file classes"
from __future__ import absolute_import

import ctypes
import logging
import os
import posixpath
import re
import warnings
import operator
import functools
from collections import deque

from .compatibility import FileNotFoundError, ConnectionError, PY3
from .conf import conf
from .utils import (read_block, seek_delimiter, ensure_bytes, ensure_string,
                    ensure_trailing_slash, MyNone)

logger = logging.getLogger(__name__)
_lib = None

DEFAULT_READ_BUFFER_SIZE = 2 ** 16
DEFAULT_WRITE_BUFFER_SIZE = 2 ** 26


def _nbytes(buf):
    buf = memoryview(buf)
    if PY3:
        return buf.nbytes
    return buf.itemsize * functools.reduce(operator.mul, buf.shape)


class HDFileSystem(object):
    """ Connection to an HDFS namenode

    >>> hdfs = HDFileSystem(host='127.0.0.1', port=8020)  # doctest: +SKIP
    """
    _first_pid = None

    def __init__(self, host=MyNone, port=MyNone, connect=True, autoconf=True,
                 pars=None, **kwargs):
        """
        Parameters
        ----------
        host: str; port: int
            Overrides which take precedence over information in conf files and
            other passed parameters
        connect: bool (True)
            Whether to automatically attempt to establish a connection to the
            name-node.
        autoconf: bool (True)
            Whether to use the configuration found in the conf module as
            the set of defaults
        pars : {str: str}
            any parameters for hadoop, that you can find in hdfs-site.xml,
            https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml
            This dict looks exactly like the one produced by conf - you can,
            for example, remove any problematic entries.
        kwargs: key/value
            Further override parameters.
            These are applied after the default conf and pars; the most typical
            things to set are:
            host : str (localhost)
                namenode hostname or IP address, in case of HA mode it is name
                of the cluster that can be found in "fs.defaultFS" option.
            port : int (8020)
                namenode RPC port usually 8020, in HA mode port mast be None
            user, ticket_cache, token, effective_user : str
                kerberos things
        """
        self.conf = conf.copy() if autoconf else {}
        if pars:
            self.conf.update(pars)
        self.conf.update(kwargs)
        if host is not MyNone:
            self.conf['host'] = host
        if port is not MyNone:
            self.conf['port'] = port

        self._handle = None

        if self.conf.get('ticket_cache') and self.conf.get('token'):
            m = "It is not possible to use ticket_cache and token at same time"
            raise RuntimeError(m)

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
        get_lib()
        conf = self.conf.copy()
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

        _lib.hdfsBuilderSetNameNode(o, ensure_bytes(conf.pop('host')))

        port = conf.pop('port', None)
        if port is not None:
            _lib.hdfsBuilderSetNameNodePort(o, port)

        user = conf.pop('user', None)
        if user is not None:
            _lib.hdfsBuilderSetUserName(o, ensure_bytes(user))
        effective_user = ensure_bytes(conf.pop('effective_user', None))

        ticket_cache = conf.pop('ticket_cache', None)
        if ticket_cache is not None:
            _lib.hdfsBuilderSetKerbTicketCachePath(o, ensure_bytes(ticket_cache))

        token = conf.pop('token', None)
        if token is not None:
            _lib.hdfsBuilderSetToken(o, ensure_bytes(token))

        for par, val in conf.items():
            if not _lib.hdfsBuilderConfSetStr(o, ensure_bytes(par),
                                              ensure_bytes(val)) == 0:
                warnings.warn('Setting conf parameter %s failed' % par)

        fs = _lib.hdfsBuilderConnect(o, effective_user)
        _lib.hdfsFreeBuilder(o)
        if fs:
            logger.debug("Connect to handle %d", fs.contents.filesystem)
            self._handle = fs
        else:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise ConnectionError('Connection Failed: {}'.format(msg))

    def delegate_token(self, user=None):
        """Generate delegate auth token.

        Parameters
        ----------
        user: bytes/str
            User to pass to delegation (defaults to user supplied to instance);
            this user is the only one that can renew the token.
        """
        if user is None and self.user is None:
            raise ValueError('Delegation requires a user')
        user = user or self.user
        out = _lib.hdfsGetDelegationToken(self._handle, ensure_bytes(user))
        if out:
            self.token = out
            return out
        else:
            raise RuntimeError('Token delegation failed')

    def renew_token(self, token=None):
        """
        Renew delegation token

        Parameters
        ----------
        token: str or None
            If None, uses the instance's token. It is an error to do that if
            there is no token.

        Returns
        -------
        New expiration time for the token
        """
        token = token or self.token
        if token is None:
            raise ValueError('There is no token to renew')
        return _lib.hdfsRenewDelegationToken(self._handle, ensure_bytes(token))

    def cancel_token(self, token=None):
        """
        Revoke delegation token

        Parameters
        ----------
        token: str or None
            If None, uses the instance's token. It is an error to do that if
            there is no token.
        """
        token = token or self.token
        if token is None:
            raise ValueError('There is no token to cancel')
        out = _lib.hdfsCancelDelegationToken(self._handle, ensure_bytes(token))
        if out:
            raise RuntimeError('Token cancel failed')
        if token == self.token:
            # now our token is invalid - this FS may not work
            self.token = None

    def disconnect(self):
        """ Disconnect from name node """
        if self._handle:
            logger.debug("Disconnect from handle %d",
                         self._handle.contents.filesystem)
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
        buf: int (=0)
            Client buffer size (bytes); if 0, use default.
        block_size: int
            Size of data-node blocks if writing
        """
        if not self._handle:
            raise IOError("Filesystem not connected")
        if block_size and mode != 'wb':
            raise ValueError('Block size only valid when writing new file')
        if ('a' in mode and self.exists(path) and
                replication != 0 and replication > 1):
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
        fi = self.ls(path, True)
        if deep:
            for apath in fi:
                if apath['kind'] == 'directory':
                    fi.extend(self.ls(apath['name'], True))
        if total:
            return {path: sum(f['size'] for f in fi)}
        return {p['name']: p['size'] for p in fi}

    def df(self):
        """ Used/free disc space on the HDFS system """
        cap = _lib.hdfsGetCapacity(self._handle)
        used = _lib.hdfsGetUsed(self._handle)
        return {'capacity': cap,
                'used': used,
                'percent-free': 100 * (cap - used) / cap}

    def get_block_locations(self, path, start=0, length=0):
        """ Fetch physical locations of blocks """
        if not self._handle:
            raise IOError("Filesystem not connected")
        start = int(start) or 0
        length = int(length) or self.info(path)['size']
        nblocks = ctypes.c_int(0)
        out = _lib.hdfsGetFileBlockLocations(self._handle,
                                             ensure_bytes(path),
                                             ctypes.c_int64(start),
                                             ctypes.c_int64(length),
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
        out = fi.to_dict()
        _lib.hdfsFreeFileInfo(ctypes.byref(fi), 1)
        return out

    def isdir(self, path):
        """Return True if path refers to an existing directory."""
        try:
            info = self.info(path)
            return info['kind'] == 'directory'
        except EnvironmentError:
            return False

    def isfile(self, path):
        """Return True if path refers to an existing file."""
        try:
            info = self.info(path)
            return info['kind'] == 'file'
        except EnvironmentError:
            return False

    def walk(self, path):
        """Directory tree generator, see ``os.walk``"""
        full_dirs = []
        dirs = []
        files = []

        for info in self.ls(path, True):
            name = info['name']
            tail = posixpath.split(name)[1]
            if info['kind'] == 'directory':
                full_dirs.append(name)
                dirs.append(tail)
            else:
                files.append(tail)

        yield path, dirs, files

        for d in full_dirs:
            for res in self.walk(d):
                yield res

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
            root = path[:ind + 1]
        else:
            root = '/'
        allpaths = []
        for dirname, dirs, fils in self.walk(root):
            allpaths.extend(posixpath.join(dirname, d) for d in dirs)
            allpaths.extend(posixpath.join(dirname, f) for f in fils)
        pattern = re.compile("^" + path.replace('//', '/')
                             .rstrip('/')
                             .replace('*', '[^/]*')
                             .replace('?', '.') + "$")
        return [p for p in allpaths
                if pattern.match(p.replace('//', '/').rstrip('/'))]

    def ls(self, path, detail=False):
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
        fi = _lib.hdfsListDirectory(self._handle, ensure_bytes(path),
                                    ctypes.byref(num))
        out = [fi[i].to_dict() for i in range(num.value)]
        _lib.hdfsFreeFileInfo(fi, num.value)
        if detail:
            return out
        else:
            return [o['name'] for o in out]

    @property
    def host(self):
        return self.conf.get('host', '')

    @property
    def port(self):
        return self.conf.get('port', '')

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
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Create directory failed: {}'.format(msg))

    def makedirs(self, path, mode=0o711):
        """ Create directory together with any necessary intermediates """
        out = _lib.hdfsCreateDirectoryEx(self._handle, ensure_bytes(path),
                                         ctypes.c_short(mode), 1)
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Create directory failed: {}'.format(msg))

    def set_replication(self, path, replication):
        """ Instruct HDFS to set the replication for the given file.

        If successful, the head-node's table is updated immediately, but
        actual copying will be queued for later. It is acceptable to set
        a replication that cannot be supported (e.g., higher than the
        number of data-nodes).
        """
        if replication < 0:
            raise ValueError('Replication must be positive,'
                             ' or 0 for system default')
        out = _lib.hdfsSetReplication(self._handle, ensure_bytes(path),
                                      ctypes.c_int16(int(replication)))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Set replication failed: {}'.format(msg))

    def mv(self, path1, path2):
        """ Move file at path1 to path2 """
        if not self.exists(path1):
            raise FileNotFoundError(path1)
        out = _lib.hdfsRename(self._handle, ensure_bytes(path1),
                              ensure_bytes(path2))
        return out == 0

    def concat(self, destination, paths):
        """Concatenate inputs to destination

        Source files *should* all have the same block size and replication.
        The destination file must be in the same directory as
        the source files. If the target exists, it will be appended to.

        Some HDFSs impose that the target file must exist and be an exact
        number of blocks long, and that each concated file except the last
        is also a whole number of blocks.

        The source files are deleted on successful
        completion.
        """
        if not self.exists(destination):
            self.touch(destination)
        arr = (ctypes.c_char_p * (len(paths) + 1))()
        arr[:-1] = [ensure_bytes(s) for s in paths]
        arr[-1] = ctypes.c_char_p()  # NULL pointer
        out = _lib.hdfsConcat(self._handle, ensure_bytes(destination), arr)
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Concat failed on %s %s' % (destination, msg))

    def rm(self, path, recursive=True):
        "Use recursive for `rm -r`, i.e., delete directory and contents"
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsDelete(self._handle, ensure_bytes(path), bool(recursive))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Remove failed on %s %s' % (path, msg))

    def exists(self, path):
        """ Is there an entry at path? """
        out = _lib.hdfsExists(self._handle, ensure_bytes(path))
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
        Make read/writeable to all
        >>> hdfs.chmod('/path/to/file', 0o777)  # doctest: +SKIP

        Make read/writeable only to user
        >>> hdfs.chmod('/path/to/file', 0o700)  # doctest: +SKIP

        Make read-only to user
        >>> hdfs.chmod('/path/to/file', 0o100)  # doctest: +SKIP
        """
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChmod(self._handle, ensure_bytes(path),
                             ctypes.c_short(mode))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError("chmod failed on %s %s" % (path, msg))

    def chown(self, path, owner, group):
        """ Change owner/group """
        if not self.exists(path):
            raise FileNotFoundError(path)
        out = _lib.hdfsChown(self._handle, ensure_bytes(path),
                             ensure_bytes(owner), ensure_bytes(group))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError("chown failed on %s %s" % (path, msg))

    def cat(self, path):
        """ Return contents of file """
        if not self.exists(path):
            raise FileNotFoundError(path)
        with self.open(path, 'rb') as f:
            result = f.read()
        return result

    def get(self, hdfs_path, local_path, blocksize=DEFAULT_READ_BUFFER_SIZE):
        """ Copy HDFS file to local """
        # TODO: _lib.hdfsCopy() may do this more efficiently
        if not self.exists(hdfs_path):
            raise FileNotFoundError(hdfs_path)
        with self.open(hdfs_path, 'rb') as f:
            with open(local_path, 'wb') as f2:
                out = 1
                while out:
                    out = f.read(blocksize)
                    f2.write(out)

    def getmerge(self, path, filename, blocksize=DEFAULT_READ_BUFFER_SIZE):
        """ Concat all files in path (a directory) to local output file """
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath, 'rb') as f:
                    out = 1
                    while out:
                        out = f.read(blocksize)
                        f2.write(out)

    def put(self, filename, path, chunk=DEFAULT_WRITE_BUFFER_SIZE, replication=0, block_size=0):
        """ Copy local file to path in HDFS """
        with self.open(path, 'wb', replication=replication,
                       block_size=block_size) as target:
            with open(filename, 'rb') as source:
                while True:
                    out = source.read(chunk)
                    if len(out) == 0:
                        break
                    target.write(out)

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

    def list_encryption_zones(self):
        """Get list of all the encryption zones"""
        x = ctypes.c_int(8)
        out = _lib.hdfsListEncryptionZones(self._handle, x)
        if not out:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError("EZ listing failed: %s" % msg)

        res = [out[i].to_dict() for i in range(x.value)]
        if res:
            _lib.hdfsFreeEncryptionZoneInfo(out, x)
        return res

    def create_encryption_zone(self, path, key_name):
        out = _lib.hdfsCreateEncryptionZone(self._handle, ensure_bytes(path),
                                            ensure_bytes(key_name))
        if out != 0:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError("EZ create failed: %s %s" % (path, msg))


def get_lib():
    """ Import C-lib only on demand """
    global _lib
    if _lib is None:
        from .lib import _lib as l
        _lib = l


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
            raise NotImplementedError("Opening a file in text mode is not"
                                      " supported, use ``io.TextIOWrapper``.")
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
        self.size = self.info()['size']

    def _set_handle(self):
        out = _lib.hdfsOpenFile(self._fs, ensure_bytes(self.path),
                                mode_numbers[self.mode], self.buff,
                                ctypes.c_short(self.replication),
                                ctypes.c_int64(self.block_size))
        if not out:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError("Could not open file: %s, mode: %s %s" %
                          (self.path, self.mode, msg))
        self._handle = out

    def readinto(self, length, out):
        """
        Read up to ``length`` bytes from the file into the ``out`` buffer,
        which can be of any type that implements the buffer protocol (example: ``bytearray``,
        ``memoryview`` (py3 only), numpy array, ...).

        Parameters
        ----------
        length : int
            maximum number of bytes to read

        out : buffer
            where to write the output data

        Returns
        -------
        int
            number of bytes read
        """
        if not _lib.hdfsFileIsOpenForRead(self._handle):
            raise IOError('File not in read mode')
        bufsize = length
        bufpos = 0

        # convert from buffer protocol to ctypes-compatible type
        buflen = _nbytes(out)
        buf_for_ctypes = (ctypes.c_byte * buflen).from_buffer(out)

        while length:
            bufp = ctypes.byref(buf_for_ctypes, bufpos)
            ret = _lib.hdfsRead(
                self._fs, self._handle, bufp, ctypes.c_int32(bufsize - bufpos))
            if ret == 0:  # EOF
                break
            if ret > 0:
                length -= ret
                bufpos += ret
            else:
                raise IOError('Read file %s Failed:' % self.path, -ret)
        return bufpos

    def read(self, length=None, out_buffer=None):
        """
        Read up to ``length`` bytes from the file. Reads shorter than ``length``
        only occur at the end of the file, if less data is available.

        If ``out_buffer`` is given, read directly into ``out_buffer``. It can be
        anything that implements the buffer protocol, for example ``bytearray``,
        ``memoryview`` (py3 only), numpy arrays, ...

        Parameters
        ----------

        length : int
            number of bytes to read. if it is None, read all remaining bytes
            from the current position.

        out_buffer : buffer, None or True
            the buffer to use as output, None to return bytes, True to create
            and return new buffer

        Returns
        -------
        bytes
            the data read (only if out_buffer is None)

        memoryview
            the data read as a memoryview into the buffer
        """
        return_buffer = out_buffer is not None
        max_read = self.size - self.tell()
        read_length = max_read if length in [None, -1] else length
        read_length = min(max_read, read_length)

        if out_buffer is None or out_buffer is True:
            out_buffer = bytearray(read_length)
        else:
            if _nbytes(out_buffer) < read_length:
                raise IOError('buffer too small (%d < %d)' % (_nbytes(out_buffer), read_length))

        bytes_read = self.readinto(length=read_length, out=out_buffer)

        if bytes_read < _nbytes(out_buffer):
            out_buffer = memoryview(out_buffer)[:bytes_read]

        if return_buffer:
            return memoryview(out_buffer)
        return memoryview(out_buffer).tobytes()

    def readline(self, chunksize=0, lineterminator='\n'):
        """ Return a line using buffered reading.

        A line is a sequence of bytes between ``'\n'`` markers (or given
        line-terminator).

        Line iteration uses this method internally.

        Note: this function requires many calls to HDFS and is slow; it is
        in general better to wrap an HDFile with an ``io.TextIOWrapper`` for
        buffering, text decoding and newline support.
        """
        if chunksize == 0:
            chunksize = self.buff if self.buff != 0 else DEFAULT_READ_BUFFER_SIZE
        lineterminator = ensure_bytes(lineterminator)
        start = self.tell()
        seek_delimiter(self, lineterminator, chunksize, allow_zero=False)
        end = self.tell()
        self.seek(start)
        return self.read(end - start)

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

    def __next__(self):
        """ Enables reading a file as a buffer in pandas """
        out = self.readline()
        if out:
            return out
        else:
            raise StopIteration

    # PY2 compatibility
    next = __next__

    def readlines(self):
        """ Return all lines in a file as a list """
        return list(self)

    def tell(self):
        """ Get current byte location in a file """
        out = _lib.hdfsTell(self._fs, self._handle)
        if out == -1:
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
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
        if out == -1:  # pragma: no cover
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('Seek Failed on file %s' % (self.path, msg))
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
            msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
            raise IOError('File not write mode: {}'.format(msg))
        write_block = self.buff if self.buff != 0 else DEFAULT_WRITE_BUFFER_SIZE
        for offset in range(0, len(data), write_block):
            d = ensure_bytes(data[offset:offset + write_block])
            if not _lib.hdfsWrite(self._fs, self._handle, d, len(d)) == len(d):
                msg = ensure_string(_lib.hdfsGetLastError()).split('\n')[0]
                raise IOError('Write failed on file %s, %s' % (self.path, msg))
        return len(data)

    def flush(self):
        """ Send buffer to the data-node; actual write may happen later """
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
