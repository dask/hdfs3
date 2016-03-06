Installation
============

Conda
-----

Both the compiled ``libhdfs3`` library (and its dependencies) and the ``hdfs3``
Python project are available via conda on the ``dask`` channel:

.. code-block:: bash

   $ conda install hdfs3 -c dask

Note that packages are only available for the ``linux-64`` platform.

PyPI and apt-get
----------------

Alternatively you can install the ``libhdfs3.so`` library using a system
installer like ``apt-get``::

    echo "deb https://dl.bintray.com/wangzw/deb trusty contrib" | sudo tee /etc/apt/sources.list.d/bintray-wangzw-deb.list
    sudo apt-get install -y apt-transport-https
    sudo apt-get update
    sudo apt-get install libhdfs3 libhdfs3-dev

And install the Python wrapper library, ``hdfs3``, with ``pip``::

    pip install hdfs3

Build from Source
-----------------

See the `libhdfs3 installation instructions`_ to install the compiled library.

.. _`libhdfs3 installation instructions`: https://github.com/PivotalRD/libhdfs3#installation

You can download the ``hdfs3`` Python wrapper library from github and install
normally::

   git clone git@github.com:dask/hdfs3
   cd hdfs3
   python setup.py install
