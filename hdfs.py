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
        
    def get_block_locations(self, path, start=0, length=None):
        "Fetch physical locations of blocks"
        assert self._handle, "Filesystem not connected"
        fi = self.info(path)
        length = length or fi.size
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
        return lib.hdfsGetPathInfo(self._handle, ensure_byte(path)).contents
    
    def __repr__(self):
        state = ['Disconnected', 'Connected'][self._handle is not None]
        return 'hdfs://%s:%s, %s' % (self.host, self.port, state)
    
    def __del__(self):
        if self._handle:
            self.disconnect()
    
    # TODO: other typical filesystem tasks


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
        m = {'w': 1, 'r': 0}[mode]
        self.mode = mode
        out = lib.hdfsOpenFile(self._fs, ensure_byte(path), m, buff,
                            ctypes.c_short(repl), ctypes.c_int64(offset))
        if out == 0:
            raise IOError("File open failed")
        self._handle = out
    
    def read(self, length):
        assert lib.hdfsFileIsOpenForRead(self._handle), 'File not read mode'
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
        return 'hdfs://%s:%s%s, mode %s' % (self.fs.host, self.fs.port,
                                            self.path, self.mode)

def test():
    fs = HDFileSystem()
    fs.connect()
    print(fs)
    f = fs.open('/newtest', 'w', repl=2)
    print(f)
    f.write(b'a' * (129 * 2**20))
    f.close()
    print(f)
    print(subprocess.check_output("hadoop fs -ls /newtest", shell=True))
    print(f.get_block_locs())
