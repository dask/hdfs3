Known Limitations
=================

Forked processes
----------------

The ``libhdfs3`` library may fail when an ``HDFileSystem`` is copied to a new
forked process.  This happens in some cases when using ``hdfs3`` with
``multiprocessing`` in Python 2.  Common solutions include the following:

*  Use threads
*  Use Python 3 and a multiprocessing context using spawn with
   ``multiprocessing.get_context(method='spawn')`` see `multiprocessing docs`_
*  Only instantiate ``HDFileSystem`` within the forked processes, do not start
   an ``HDFileSystem`` within the parent processes or do not use that
   ``HDFileSystem`` within the child processes.
*  Use a file based lock.  We recommend ``locket``::

        $ pip install locket

    .. code-block:: python

        import locket

        with locket.lock_file('.lock'):
            # do hdfs3 work


.. _`multiprocessing docs`: https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
