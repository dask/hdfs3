from __future__ import print_function, division, absolute_import

import warnings

from dask.base import tokenize
from dask.bytes import core

from .core import HDFileSystem

warnings.warn("DeprecationWarning: `hdfs3.dask` is deprecated and will be "
              "removed in the next release. Dask hdfs functionality has "
              "migrated to `dask.bytes.hdfs`. Please update your version of "
              "dask to > 0.16.1")


class DaskHDFileSystem(HDFileSystem):
    sep = '/'

    def mkdirs(self, path):
        return super(DaskHDFileSystem, self).makedirs(path)

    def ukey(self, path):
        return tokenize(path, self.info(path)['last_mod'])

    def size(self, path):
        return self.info(path)['size']

    def _get_pyarrow_filesystem(self):
        from ._pyarrow import HDFSWrapper
        return HDFSWrapper(self)


core._filesystems['hdfs'] = DaskHDFileSystem
