HDFS Configuration
==================

Defaults
--------

This library tries to find ``core-site.xml`` and ``hdfs-site.xml`` in typical
locations and reads default configuration parameters from there.  They may also
be specified manually when constructing the ``HDFileSystem`` object.

Short-circuit reads in HDFS
---------------------------

Typically in HDFS, all data reads go through the datanode. Alternatively, a
process that runs on the same node as the data can bypass or `short-circuit`
the communication path through the datanode and instead read directly from a
file.

HDFS and ``hdfs3`` can be configured for short-circuit reads using the
following two steps:

* Set the ``LIBHDFS3_CONF`` environment variable to the location of the
  ``hdfs-site.xml`` configuration file (e.g.,
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

The above configuration changes should allow for short-circuit reads. If you
continue to receive warnings to ``retry the same node but disable read
shortcircuit feature``, check the above settings. Note that the HDFS reads
should still function despite the warning, but performance might be impacted.

For more information about configuring short-circuit reads, refer to the
`HDFS Short-Circuit Local Reads`_ documentation.

.. _`HDFS Short-Circuit Local Reads`: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html
