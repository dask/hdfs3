Quickstart
----------

Install `hdfs3` and its dependencies:

.. code-block:: bash

   $ conda install hdfs3 -c blaze

Import `hdfs3` and connect to an HDFS cluster:

.. code-block:: python

   >>> from hdfs3 import HDFileSystem
   >>> hdfs = HDFileSystem(host='localhost', port=8020)

Interact with files on HDFS:

.. code-block:: python

   >>> hdfs.ls('/user/data')
   
   >>> hdfs.put('local-file.txt', '/user/data/file.txt')
   
   >>> hdfs.cp('/user/data/file.txt', '/user2/data')

Read data directly from HDFS into a local Python process:

.. code-block:: python

   >>> with hdfs.open('/user/data/file.txt') as f:
           f.seek(2000)
           bytes = f.read(1000000)
