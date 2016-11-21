Known Limitations
=================

Forked processes
----------------

The ``libhdfs3`` library is not fork-safe.  If you start using ``hdfs3``
in a parent process and then fork a child process, using the library from
the child process may produce random errors because of system resources
that will not be available (such as background threads).  Common solutions
include the following:

*  Use threads instead of processes
*  Use Python 3 and a multiprocessing context using either the "spawn" or
   "forkserver" method (see `multiprocessing docs`_)
*  Only instantiate ``HDFileSystem`` within the forked processes, do not
   ever start an ``HDFileSystem`` within the parent processes

.. _`multiprocessing docs`: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
