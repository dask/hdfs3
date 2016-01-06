Welcome to hdfs3's documentation!
=================================

Python wrapper for libhdfs3, a native library to interact with the Hadoop File
System.

The `Hadoop File System`_ (HDFS) is a widely deployed, distributed, data-local
file system written in Java.  libhdfs3_ is an alternative native C/C++ HDFS
client that interacts with HDFS without the JVM.  This library, `hdfs3`, is a
Python wrapper around the C/C++ libhdfs3 library.  It provides both direct
access to libhdfs3 from Python as well as a typical Pythonic interface.

API
---

.. currentmodule:: hdfs3.core

.. autoclass:: HDFileSystem
   :members:

.. autoclass:: HDFile
   :members:


.. _`Hadoop File System`: https://en.wikipedia.org/wiki/Apache_Hadoop
.. _libhdfs3: http://pivotalrd.github.io/libhdfs3/
