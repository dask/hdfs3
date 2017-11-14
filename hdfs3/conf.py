from __future__ import absolute_import

import os
import re
import warnings
from .compatibility import FileNotFoundError

# standard defaults
conf_defaults = {'host': 'localhost', 'port': 8020}
conf = conf_defaults.copy()


def hdfs_conf(confd, more_files=None):
    """ Load HDFS config from default locations.

    Parameters
    ----------
    confd: str
        Directory location to search in
    more_files: list of str or None
        If given, additional filenames to query
    """
    files = ['core-site.xml', 'hdfs-site.xml']
    if more_files:
        files.extend(more_files)
    c = {}
    for afile in files:
        try:
            c.update(conf_to_dict(os.sep.join([confd, afile])))
        except FileNotFoundError:
            pass
    if not c:
        # no config files here
        return
    if 'fs.defaultFS' in c and c['fs.defaultFS'].startswith('hdfs'):
        # default FS in 'core'
        text = c['fs.defaultFS']
        if text.startswith('hdfs://'):
            text = text[7:]
        host = text.split(':', 1)[0]
        port = text.split(':', 1)[1:]
        if host:
            c['host'] = host
        if port:
            c['port'] = int(port[0])
    if 'dfs.namenode.rpc-address' in c:
        # name node address
        text = c['dfs.namenode.rpc-address']
        host = text.split(':', 1)[0]
        port = text.split(':', 1)[1:]
        if host:
            c['host'] = host
        if port:
            c['port'] = int(port[0])
    if c.get("dfs.nameservices", None):
        # HA override
        c['host'] = c["dfs.nameservices"].split(',', 1)[0]
        c['port'] = None
    if 'host' not in conf:
        # no host found at all, config cannot work, so warn
        warnings.warn('No host found in HDFS config')
        conf['host'] = ''
    conf.clear()
    conf.update(c)


def reset_to_defaults():
    conf.clear()
    conf.update(conf_defaults)


def conf_to_dict(fname):
    """ Read a hdfs-site.xml style conf file, produces dictionary """
    name_match = re.compile("<name>(.*?)</name>")
    val_match = re.compile("<value>(.*?)</value>")
    conf = {}
    for line in open(fname):
        name = name_match.search(line)
        if name:
            key = name.groups()[0]
        val = val_match.search(line)
        if val:
            val = val.groups()[0]
            conf[key] = val
    return conf


def guess_config():
    """ Look for config files in common places """
    d = None
    if 'LIBHDFS3_CONF' in os.environ:
        if not os.path.exists(os.environ['LIBHDFS3_CONF']):
            os.environ.pop('LIBHDFS3_CONF', None)
        fdir, fn = os.path.split(os.environ['LIBHDFS3_CONF'])
        hdfs_conf(fdir, more_files=[fn])
        return
    elif 'HADOOP_CONF_DIR' in os.environ:
        d = os.environ['HADOOP_CONF_DIR']
    elif 'HADOOP_INSTALL' in os.environ:
        d = os.environ['HADOOP_INSTALL'] + '/hadoop/conf'
    if d is None:
        # list of potential typical system locations
        for loc in ['/etc/hadoop/conf']:
            if os.path.exists(loc):
                fns = os.listdir(loc)
                if 'hdfs-site.xml' in fns:
                    d = loc
                    break
    if d is None:
        # fallback: local dir
        d = os.getcwd()
    hdfs_conf(d)
    if os.path.exists(os.path.join(d, 'hdfs-site.xml')):
        os.environ['LIBHDFS3_CONF'] = os.path.join(d, 'hdfs-site.xml')


guess_config()
