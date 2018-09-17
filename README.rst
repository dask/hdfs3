hdfs3
=====

|Build Status|

This project is not undergoing development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pyarrow's JNI `hdfs interface`_ is mature and stable. It also has fewer problems
with configuration and various security settings, and does not require the complex
build process of libhdfs3. Therefore, all users who have trouble with hdfs3 are
recommended to try pyarrow.

.. _hdfs interface: https://arrow.apache.org/docs/python/filesystems.html#hadoop-file-system-hdfs

Old README
~~~~~~~~~~

hdfs3 is a lightweight Python wrapper for libhdfs3_, a native C/C++ library to interact with the Hadoop File System (HDFS).

View the documentation_ for hdfs3.

.. _libhdfs3: https://github.com/ContinuumIO/libhdfs3-downstream/tree/master/libhdfs3
.. _documentation: https://hdfs3.readthedocs.io/en/latest/

.. |Build Status| image:: https://travis-ci.org/dask/hdfs3.svg?branch=master
    :target: https://travis-ci.org/dask/hdfs3
