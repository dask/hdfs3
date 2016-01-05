from hdfs3 import HDFileSystem

def test_example():
    import time
    import os
    os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

    fs = HDFileSystem(host='localhost', port=8020)
    print(fs)

    data = b'a' * (10 * 2**20)

    with fs.open('/tmp/test', 'w', repl=1) as f:
        t0 = time.time()
        f.write(data)
    t1 = time.time()
    print(t1 - t0)

    with fs.open('/tmp/test', 'r') as f:
        out = f.read(len(data))
        assert len(data) == len(out)
        assert out == data
    print(time.time() - t1)
