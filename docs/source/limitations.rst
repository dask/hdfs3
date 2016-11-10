Known Limitations
=================

Forked processes
----------------

The ``libhdfs3`` library may fail when an ``HDFileSystem`` is copied to a new
forked process.  This happens in some cases when using ``hdfs3`` with
``multiprocessing`` in Python 2.

We get around this by using file-based locks, which slightly limit concurrency.
