HDFS3
=====

A Python wrapper for ``libhdfs3``, a native client library for the Hadoop
File System.

The `Hadoop File System`_ (HDFS) is a widely deployed, distributed, data-local
file system written in Java.  Pivotal produced libhdfs3_, an alternative native
C/C++ HDFS client that interacts with HDFS without the JVM.

The ``hdfs3`` library, is a lightweight Python wrapper around the C/C++
``libhdfs3`` library.  It provides both direct access to libhdfs3 from Python
as well as a typical Pythonic interface.

Example
-------

HDFS3 is easy to setup:

.. code-block:: python

   from hdfs3 import HDFileSystem
   hdfs = HDFileSystem(host='localhost', port=8020)

Use it to manage files on HDFS:

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

Both the compiled ``libhdfs3`` library (and its dependencies) and the ``hdfs3``
Python project are available via conda on the ``blaze`` channel::

   conda install -c blaze hdfs3

Note that packages are only available for the ``linux-64`` platform.


Motivation
----------

We choose to use an alternative C/C++/Python HDFS client rather than the
default JVM client for convenience and performance reasons:

*  Convenience: Interactions between Java libraries and Native (C/C++/Python)
   libraries can be cumbersome and causes frustration in development,
   maintenance, and debugging.
*  Performance: Native libraries like ``libhdfs3`` do not suffer the long JVM
   startup times, improving interaction.


Short-circuit reads in HDFS
---------------------------

Typically in HDFS, all data reads go through the datanode. Alternatively, a
process that runs on the same node as the data can bypass or `short-circuit`
the communication path through the datanode and instead read directly from a
file.

HDFS and ``hdfs3`` can be configured for short-circuit reads using the
following two steps:

* Set the ``LIBHDFS3_CONF`` environment variable to the location of the
  hdfs-site.xml configuration file (e.g.,
  ``export LIBHDFS3_CONF=/etc/hadoop/conf/hdfs-site.xml``).

* Configure the appropriate settings in ``hdfs-site.xml`` on all of the HDFS nodes:

.. code-block:: xml

  <configuration>
    <property>
      <name>dfs.client.read.shortcircuit</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.domain.socket.path</name>
      <value>/var/lib/hadoop-hdfs/dn_socket</value>
    </property>
  </configuration>

Note that you might get HDFS warnings similar to ``retry the same node but
disable read shortcircuit feature`` without the above two configuration
changes, but the reads should still function.

For more information about configuring short-circuit reads, refer to the
`HDFS Short-Circuit Local Reads
<https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html>`_ documentation.


Related Work
------------

* libhdfs3_: The underlying C++ library
* snakebite_: Another Python HDFS library using Protobufs


API
---

.. currentmodule:: hdfs3.core

.. autoclass:: HDFileSystem
   :members:

.. autoclass:: HDFile
   :members:


.. _`Hadoop File System`: https://en.wikipedia.org/wiki/Apache_Hadoop
.. _libhdfs3: http://pivotalrd.github.io/libhdfs3/
.. _snakebite: http://snakebite.readthedocs.org/en/latest/
