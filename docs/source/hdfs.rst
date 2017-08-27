HDFS Configuration
==================

Defaults
--------

Several methods are available for configuring HDFS3.

The simplest is to load values from ``core-site.xml`` and ``hdfs-site.xml`` files.
HDFS3 will search typical locations and reads default configuration parameters from there.  
The file locations may also be specified with the environment variables ``HADOOP_CONF_DIR``,
which is the directory containing the XLM files, ``HADOOP_INSTALL``, in which case the 
files are expected in subdirectory ``hadoop/conf/`` or ``LIBHDFS3_CONF``, which should
explicitly point to the ``hdfs-site.xml`` file you wish to use.

It is also possible to pass parameters to HDFS3 when instantiating the file system. You
can either provide individual common overrides (e.g., ``host='myhost'``) or provide
a whole configuration as a dictionary (``pars={}``) with the same key names as typically
contained in the XML config files. These parameters will take precedence over any loaded
from files, or you can disable using the default configuration at all with ``autoconf=False``.

The special environment variable ``LIBHDFS3_CONF`` will be automatically set when parsing
the config files, if possible. Since the library is only loaded upon the first instantiation
of a HDFileSystem, you still have the option to change its value in ``os.environ``.

Short-circuit reads in HDFS
---------------------------

Typically in HDFS, all data reads go through the datanode. Alternatively, a
process that runs on the same node as the data can bypass or `short-circuit`
the communication path through the datanode and instead read directly from a
file.

HDFS and ``hdfs3`` can be configured for short-circuit reads. The easiest method is
to edit the ``hdfs-site.xml`` file whose location you specify as above.

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

The above configuration changes should allow for short-circuit reads. If you
continue to receive warnings to ``retry the same node but disable read
shortcircuit feature``, check the above settings. Note that the HDFS reads
should still function despite the warning, but performance might be impacted.

For more information about configuring short-circuit reads, refer to the
`HDFS Short-Circuit Local Reads`_ documentation.

.. _`HDFS Short-Circuit Local Reads`: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html

High-availability mode
----------------------

Although HDFS is resilient to failure of data-nodes, the name-node is a single
repository of metadata for the system, and so a single point of failure.
High-availability (HA) involves configuring fall-back name-nodes which can take
over in the event of failure. A good description han be found `here`_.

.. _`Cloudera and HDFS HA`: https://www.cloudera.com/documentation/enterprise/5-8-x/topics/cdh_hag_hdfs_ha_intro.html#topic_2_1

In the case of `libhdfs3`_, the library used by hdfs3, the configuration
required for HA can be passed to the client directly in python code, or
included in configuration files, as with any other configuration options.

.. _`libhdfs HA documentation`: https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3/wiki/Configure-Parameters

In python code, this could look like the following:

.. code-block:: python

    host = "nameservice1"
    conf = {"dfs.nameservices": "nameservice1",
            "dfs.ha.namenodes.nameservice1": "namenode113,namenode188",
            "dfs.namenode.rpc-address.nameservice1.namenode113": "hostname_of_server1:8020",
            "dfs.namenode.rpc-address.nameservice1.namenode188": "hostname_of_server2:8020",
            "dfs.namenode.http-address.nameservice1.namenode188": "hostname_of_server1:50070",
            "dfs.namenode.http-address.nameservice1.namenode188": "hostname_of_server2:50070",
            "hadoop.security.authentication": "kerberos"
    }
    fs = HDFileSystem(host=host, pars=conf)

Note that no ``port`` is specified (requires hdfs version 0.1.3), it's value should be ``None``.

