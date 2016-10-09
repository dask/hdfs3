hdfs3
=====

Use HDFS natively from Python.

The `Hadoop File System`_ (HDFS) is a widely deployed, distributed, data-local
file system written in Java.  This file system backs most clusters running
Hadoop and Spark.

Pivotal produced libhdfs3_, an alternative native C/C++ HDFS client that
interacts with HDFS without the JVM, exposing first class support to non-JVM
languages like Python.

This library, ``hdfs3``, is a lightweight Python wrapper around the C/C++
``libhdfs3`` library.  It provides both direct access to libhdfs3 from Python
as well as a typical Pythonic interface.

.. code-block:: python

   >>> from hdfs3 import HDFileSystem
   >>> hdfs = HDFileSystem(host='localhost', port=8020)
   >>> hdfs.ls('/user/data')
   >>> hdfs.put('local-file.txt', '/user/data/remote-file.txt')
   >>> hdfs.cp('/user/data/file.txt', '/user2/data')

HDFS3 files comply with the Python File interface. This enables interactions
with the broader ecosystem of PyData projects.

.. code-block:: python

   >>> with hdfs.open('/user/data/file.txt') as f:
   ...     data = f.read(1000000)

   >>> with hdfs.open('/user/data/file.csv.gz') as f:
   ...     df = pandas.read_csv(f, compression='gzip', nrows=1000)


Motivation
----------

We choose to use an alternative C/C++/Python HDFS client rather than the
default JVM client for the following reasons:

*  **Convenience:** Interactions between Java libraries and Native
   (C/C++/Python) libraries can be cumbersome.  Using a native library from
   Python smoothes over the experience in development, maintenance, and
   debugging.
*  **Performance:** Native libraries like ``libhdfs3`` do not suffer the long JVM
   startup times, improving interaction.


Related Work
------------

* libhdfs3_: The underlying C++ library
* snakebite_: Another Python HDFS library using Protobufs
* Dask_: Parent project, a parallel computing library in Python
* Dask.distributed_: Distributed computing in Python

.. toctree::
   :maxdepth: 1
   :hidden:

   install
   quickstart
   examples-overview
   hdfs
   api
   limitations

.. _`Hadoop File System`: https://en.wikipedia.org/wiki/Apache_Hadoop
.. _libhdfs3: http://pivotalrd.github.io/libhdfs3/
.. _snakebite: https://snakebite.readthedocs.io/en/latest/
.. _Dask: http://dask.pydata.org/en/latest/
.. _Dask.distributed: https://distributed.readthedocs.io/en/latest/
