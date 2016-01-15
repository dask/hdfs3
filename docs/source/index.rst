hdfs3
=====

A Python wrapper for ``libhdfs3``, a native client library for the Hadoop
File System.

The `Hadoop File System`_ (HDFS) is a widely deployed, distributed, data-local
file system written in Java.  Pivotal produced libhdfs3_, an alternative native
C/C++ HDFS client that interacts with HDFS without the JVM.

The ``hdfs3`` library, is a lightweight Python wrapper around the C/C++
``libhdfs3`` library.  It provides both direct access to libhdfs3 from Python
as well as a typical Pythonic interface.

.. code-block:: python

   from hdfs3 import HDFileSystem
   hdfs = HDFileSystem(host='localhost', port=8020)

   hdfs.ls('/user/data')
   
   hdfs.put('local-file.txt', '/user/data/remote-file.txt')
   
   hdfs.cp('/user/data/file.txt', '/user2/data')

   with hdfs.open('/user/data/file.txt') as f:
       f.seek(2000)
       bytes = f.read(1000000)

Refer to the following documentation to get started with `hdfs3`:

* :doc:`install`
* :doc:`quickstart`


Motivation
----------

We choose to use an alternative C/C++/Python HDFS client rather than the
default JVM client for convenience and performance reasons:

*  Convenience: Interactions between Java libraries and Native (C/C++/Python)
   libraries can be cumbersome and causes frustration in development,
   maintenance, and debugging.
*  Performance: Native libraries like ``libhdfs3`` do not suffer the long JVM
   startup times, improving interaction.


Related Work
------------

* libhdfs3_: The underlying C++ library
* snakebite_: Another Python HDFS library using Protobufs

.. toctree::
   :maxdepth: 1
   :hidden:

   install
   quickstart
   cli
   hdfs
   api

.. _`Hadoop File System`: https://en.wikipedia.org/wiki/Apache_Hadoop
.. _libhdfs3: http://pivotalrd.github.io/libhdfs3/
.. _snakebite: http://snakebite.readthedocs.org/en/latest/
