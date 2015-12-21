"Simple example of writing to a HDFS file with libhdfs3"

import ctypes
lib = ctypes.cdll.LoadLibrary('./libhdfs3.so')
o = lib.hdfsNewBuilder()
lib.hdfsBuilderSetNameNodePort(o, 9000)
host = open('/etc/hadoop/conf/masters').readlines()[1][:-1]
if hasattr(host, 'encode'):
    host = host.encode('ascii')
lib.hdfsBuilderSetNameNode(o, host)
fs = lib.hdfsBuilderConnect(o)
import time
import subprocess

def tryme(N, repeats=1, filename='/binfile', repl=3, offset=0):
    data = b'a' * N
    t0 = time.time()
    # mode=1 -> create/write
    f = lib.hdfsOpenFile(fs, filename, 1, 0, repl, offset)
    if lib.hdfsFileIsOpenForWrite(f):
        for i in range(repeats):
            assert lib.hdfsWrite(fs, f, data, len(data)) == len(data)
        assert lib.hdfsFlush(fs, f) == 0
        assert lib.hdfsCloseFile(fs, f) == 0
    t = time.time() - t0
    print(test(filename))
    return t

def test(filename='/binfile'):
    return subprocess.check_output("hadoop fs -ls " + filename, shell=True)
