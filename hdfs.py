# -*- coding: utf-8 -*-
"Main module defining filesystem and file classes"
import os
import ctypes
import sys
import subprocess
import warnings
PY3 = sys.version_info.major > 2
lib = ctypes.cdll.LoadLibrary('./libhdfs3.so')


def get_default_host():
    "Try to guess the namenode by looking in this machine's hadoop conf."
    confd = os.environ.get('HADOOP_CONF_DIR', os.environ.get('HADOOP_INSTALL',
                           '') + '/hadoop/conf')
    host = open(os.sep.join([confd, 'masters'])).readlines()[1][:-1]
    return host


def ensure_byte(s):
    "Give strings that ctypes is guaranteed to handle"
    if PY3:
        if isinstance(s, str):
            return s.encode('ascii')
        elif isinstance(s, bytes):
            return s
        else:
            raise ValueError('Could not convert %s to bytes' % s)
    else:  # in PY2, strings are fine for ctypes
        return s


def init_kerb():
    """Uses system kinit to find credentials. Set up by editing
    krb5.conf"""
    raise NotImplementedError("Please set your credentials manually")
    out1 = subprocess.check_call(['kinit'])
    out2 = subprocess.check_call(['klist'])
    HDFileSystem.ticket_cache = None
    HDFileSystem.token = None


class HDFileSystem():
    "A connection to an HDFS namenode"
    _handle = None
    host = get_default_host()
    port = 9000
    user = None
    ticket_cache = None
    token = None
    pars = {}
    _token = None  # Delegation token (generated)
    autoconnect = True
    
    def __init__(self, **kwargs):
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
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])
        # self.__dict__.update(kwargs)
        if self.autoconnect:
            self.connect()
    
    def connect(self):
        assert self._handle is None, "Already connected"
        o = lib.hdfsNewBuilder()
        lib.hdfsBuilderSetNameNodePort(o, self.port)
        lib.hdfsBuilderSetNameNode(o, ensure_byte(self.host))
        if self.user:
            lib.hdfsBuilderSetUserName(o, ensure_byte(self.user))
        if self.ticket_cache:
            lib.hdfsBuilderSetKerbTicketCachePath(o, ensure_byte(self.ticket_cache))
        if self.token:
            lib.hdfsBuilderSetToken(o, ensure_byte(self.token))
        if self.pars:
            for par in self.pars:
                try:
                    assert lib.hdfsBuilderConfSetStr(o, ensure_byte(par),
                                          ensure_byte(self.pars(par))) == 0
                except AssertionError:
                    warnings.warn('Setting conf parameter %s failed' % par)
        fs = lib.hdfsBuilderConnect(o)
        if fs:
            self._handle = fs
            #if self.token:   # TODO: find out what a delegation token is
            #    self._token = lib.hdfsGetDelegationToken(self._handle,
            #                                             ensure_byte(self.user))
        else:
            raise RuntimeError('Connection Failed')
    
    def disconnect(self):
        if self._handle:
            lib.hdfsDisconnect(self._handle)
        self._handle = None
    
    def open(self, path, mode='r', **kwargs):
        assert self._handle, "Filesystem not connected"
        return HDFile(self, path, mode, **kwargs)
        
    def du(self, path, total=False, deep=False):
        if isinstance(total, str):
            total = total=='True'
        if isinstance(deep, str):
            deep = deep=='True'
        fi = self.ls(path)
        if deep:
            for apath in fi:
                if apath['kind'] == 68:  # directory
                    fi.extend(self.ls(apath['name']))
        if total:
            return {path: sum(f['size'] for f in fi)}
        return {p['name']: p['size'] for p in fi}
    
    def df(self):
        cap = lib.hdfsGetCapacity(self._handle)
        used = lib.hdfsGetUsed(self._handle)
        return {'capacity': cap, 'used': used, 'free%': 100*(cap-used)/cap}
    
    def get_block_locations(self, path, start=0, length=0):
        "Fetch physical locations of blocks"
        assert self._handle, "Filesystem not connected"
        start = int(start) or 0
        length = int(length) or self.du(path)
        nblocks = ctypes.c_int(0)
        out = lib.hdfsGetFileBlockLocations(self._handle, ensure_byte(path),
                                ctypes.c_int64(start), ctypes.c_int64(length),
                                ctypes.byref(nblocks))
        locs = []
        for i in range(nblocks.value):
            block = out[i]
            hosts = [block.hosts[i] for i in
                     range(block.numOfNodes)]
            locs.append({'hosts': hosts, 'length': block.length,
                         'offset': block.offset})
        lib.hdfsFreeFileBlockLocations(out, nblocks)
        return locs

    def info(self, path):
        "File information"
        fi = lib.hdfsGetPathInfo(self._handle, ensure_byte(path)).contents
        out = struct_to_dict(fi)
        lib.hdfsFreeFileInfo(ctypes.byref(fi), 1)
        return out
    
    def ls(self, path):
        num = ctypes.c_int(0)
        fi = lib.hdfsListDirectory(self._handle, ensure_byte(path), ctypes.byref(num))
        out = [struct_to_dict(fi[i]) for i in range(num.value)]
        lib.hdfsFreeFileInfo(fi, num.value)
        return out
    
    def __repr__(self):
        state = ['Disconnected', 'Connected'][self._handle is not None]
        return 'hdfs://%s:%s, %s' % (self.host, self.port, state)
    
    def __del__(self):
        if self._handle:
            self.disconnect()
    
    def mkdir(self, path):
        out = lib.hdfsCreateDirectory(self._handle, ensure_byte(path))
        return out == 0
        
    def set_replication(self, path, repl):
        out = lib.hdfsSetReplication(self._handle, ensure_byte(path),
                                     ctypes.c_int16(int(repl)))
        return out == 0
    
    def mv(self, path1, path2):
        out = lib.hdfsRename(self._handle, ensure_byte(path1), ensure_byte(path2))
        return out == 0

    def rm(self, path, recursive=True):
        "Use recursive for `rm -r`, i.e., delete directory and contents"
        out = lib.hdfsDelete(self._handle, ensure_byte(path), bool(recursive))
        return out == 0
    
    def exists(self, path):
        out = lib.hdfsExists(self._handle, ensure_byte(path) )
        return out == 0
    
    def truncate(self, path, pos):
        # Does not appear to ever succeed
        out = lib.hdfsTruncate(self._handle, ensure_byte(path),
                               ctypes.c_int64(int(pos)), 0)
        return out == 0
    
    def chmod(self, path, mode):
        "Mode in numerical format (give as octal, if convenient)"
        out = lib.hdfsChmod(self._handle, ensure_byte(path), ctypes.c_short(int(mode)))
        return out == 0
    
    def chown(self, path, owner, group):
        "Change owner/group"
        out = lib.hdfsChown(self._handle, ensure_byte(path), ensure_byte(owner),
                            ensure_byte(group))
        return out == 0
    
    def cat(self, path):
        "Return contents of file"
        buff = b''
        with self.open(path, 'r') as f:
            out = 1
            while out:
                out = f.read(2**16)
                buff = buff + out
        return buff
    
    def get(self, path, filename):
        "Copy HDFS file to local"
        with self.open(path, 'r') as f:
            with open(filename, 'wb') as f2:
                out = 1
                while out:
                    out = f.read()
                    f2.write(out)
    
    def getmerge(self, path, filename):
        "Concat all files in path (a directory) to output file"
        files = self.ls(path)
        with open(filename, 'wb') as f2:
            for apath in files:
                with self.open(apath['name'], 'r') as f:
                    out = 1
                    while out:
                        out = f.read()
                        f2.write(out)
        

    def put(self, filename, path, chunk=2**16):
        "Copy local file to path in HDFS"
        with self.open(path, 'w') as f:
            with open(filename, 'rb') as f2:
                while True:
                    out = f2.read(chunk)
                    if len(out) == 0:
                        break
                    f.write(out)
    
    def tail(self, path, size=None):
        "Return last size bytes of file"
        size = int(size) or 1024
        length = self.du(path)
        if size > length:
            return self.cat(path)
        with self.open(path, 'r', offset=length-size) as f:
            return f.read(size)
    
    def touch(self, path):
        "Create zero-length file"
        self.open(path, 'w').close()


def struct_to_dict(s):
    return dict((name, getattr(s, name)) for (name, p) in s._fields_)

class BlockLocation(ctypes.Structure):
    _fields_ = [('corrupt', ctypes.c_int),
                ('numOfNodes', ctypes.c_int),
                ('hosts', ctypes.POINTER(ctypes.c_char_p)),
                ('names', ctypes.POINTER(ctypes.c_char_p)),
                ('topologyPaths', ctypes.POINTER(ctypes.c_char_p)),
                ('length', ctypes.c_int64),
                ('offset', ctypes.c_int64)]
lib.hdfsGetFileBlockLocations.restype = ctypes.POINTER(BlockLocation)

class FileInfo(ctypes.Structure):
    _fields_ = [('kind', ctypes.c_int8),
                ('name', ctypes.c_char_p),
                ('last_mod', ctypes.c_int64),  #time_t, could be 32bit
                ('size', ctypes.c_int64),
                ('replication', ctypes.c_short),
                ('block_size', ctypes.c_int64),
                ('owner', ctypes.c_char_p),
                ('group', ctypes.c_char_p),
                ('permissions', ctypes.c_short),  #view as octal
                ('last_access', ctypes.c_int64),  #time_t, could be 32bit               
                ]
lib.hdfsGetPathInfo.restype = ctypes.POINTER(FileInfo)
lib.hdfsListDirectory.restype = ctypes.POINTER(FileInfo)

class HDFile():
    _handle = None
    fs = None
    _fs = None
    path = None
    mode = None
    
    def __init__(self, fs, path, mode, repl=1, offset=0, buff=0):
        "Called by open on a HDFileSystem"
        self.fs = fs
        self.path = path
        self.repl = repl
        self._fs = fs._handle
        m = {'w': 1, 'r': 0, 'a': 1025}[mode]
        self.mode = mode
        out = lib.hdfsOpenFile(self._fs, ensure_byte(path), m, buff,
                            ctypes.c_short(repl), ctypes.c_int64(0))
        if out == 0:
            raise IOError("File open failed")
        self._handle = out
        if mode=='r' and offset > 0:
            self.seek(offset)
    
    def read(self, length=2**16):
        "Read, in chunks no bigger than the native filesystem (e.g., 64kb)"
        assert lib.hdfsFileIsOpenForRead(self._handle), 'File not read mode'
        # TODO: read in chunks greater than block size by multiple
        # calls to read
        # TODO: consider tell() versuss filesize to determine bytes available.
        p = ctypes.create_string_buffer(length)
        ret = lib.hdfsRead(self._fs, self._handle, p, ctypes.c_int32(length))
        if ret >= 0:
            return p.raw[:ret]
        else:
            raise IOError('Read Failed:', -ret)
    
    def tell(self):
        out = lib.hdfsTell(self._fs, self._handle)
        if out == -1:
            raise IOError('Tell Failed')
        return out
    
    def seek(self, loc):
        out = lib.hdfsSeek(self._fs, self._handle, ctypes.c_int64(loc))
        if out == -1:
            raise IOError('Seek Failed')
    
    def info(self):
        "filesystem metadata about this file"
        return self.fs.info(self.path)
    
    def write(self, data):
        data = ensure_byte(data)
        assert lib.hdfsFileIsOpenForWrite(self._handle), 'File not write mode'
        assert lib.hdfsWrite(self._fs, self._handle, data, len(data)) == len(data)
        
    def flush(self):
        lib.hdfsFlush(self._fs, self._handle)
    
    def close(self):
        self.flush()
        lib.hdfsCloseFile(self._fs, self._handle)
        self._handle = None  # libhdfs releases memory
        self.mode = 'closed'
            
    def get_block_locs(self):
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

def test():
    fs = HDFileSystem()
    with fs.open('/newtest', 'w', repl=1) as f:
        import time
        data = b'a' * (1024 * 2**20)
        t0 = time.time()
        f.write(data)
    t1 = time.time()
    with fs.open('/newtest', 'r') as f:
        out = 1
        while out:
            out = f.read(2**16)
    print(fs)
    print(f)
    print(fs.info(f.path))
    print(t1 - t0)
    print(time.time() - t1)
    print(subprocess.check_output("hadoop fs -ls /newtest", shell=True))
    print(f.get_block_locs())
    fs.rm(f.path)
