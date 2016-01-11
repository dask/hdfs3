Welcome to hdfs3's documentation!
=================================

Python wrapper for libhdfs3, a native library to interact with the Hadoop File
System.

The `Hadoop File System`_ (HDFS) is a widely deployed, distributed, data-local
file system written in Java.  libhdfs3_ is an alternative native C/C++ HDFS
client that interacts with HDFS without the JVM.  This library, ``hdfs3``, is a
Python wrapper around the C/C++ libhdfs3 library.  It provides both direct
access to libhdfs3 from Python as well as a typical Pythonic interface.

Example
-------

HDFS3 is easy to setup:

.. code-block:: python

   from hdfs3 import HDFileSystem
   hdfs = HDFileSystem(host='localhost', port=8020)

Use it to manage files on HDFS

.. code-block:: python

   >>> hdfs.ls('/user/data')
   ...
   >>> hdfs.put('local-file.txt', '/user/data/file.txt')
   >>> hdfs.cp('/user/data/file.txt', '/user2/data')

Use it to read data directly from HDFS into the local Python process:

.. code-block:: python

   with hdfs.open('/user/data/file.txt') as f:
       f.seek(2000)
       bytes = f.read(1000000)


Install
-------

Both the compiled ``libhdfs3`` library and the ``hdfs3`` Python project are
available via conda on the ``blaze`` channel::

   conda install -c blaze hdfs3


Motivation
----------

We choose to use an alternative C/C++/Python HDFS client rather than the
default JVM client for convenience and performance reasons.

*  Convenience: Interactions between Java libraries and Native (C/C++/Python)
   libraries can be cumbersome and causes frustration in development,
   maintenance, and debugging.
*  Performance: Native libraries like ``libhdfs3`` do not suffer the long JVM
   startup times, improving interaction.


API
---

.. currentmodule:: hdfs3.core

.. autoclass:: HDFileSystem
   :members:

.. autoclass:: HDFile
   :members:


.. _`Hadoop File System`: https://en.wikipedia.org/wiki/Apache_Hadoop
.. _libhdfs3: http://pivotalrd.github.io/libhdfs3/
