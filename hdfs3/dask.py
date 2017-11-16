from __future__ import print_function, division, absolute_import

from dask.base import tokenize
from dask.bytes import core
from dask.bytes.utils import infer_storage_options

from .core import HDFileSystem
from .compatibility import PY3


class DaskHDFileSystem(HDFileSystem):
    sep = '/'

    def __init__(self, **kwargs):
        kwargs2 = {k: v for k, v in kwargs.items()
                   if k in ['host', 'port', 'user', 'ticket_cache',
                            'token', 'pars']}
        super(DaskHDFileSystem, self).__init__(connect=True, **kwargs2)

    @staticmethod
    def _trim_filename(fn):
        so = infer_storage_options(fn)
        return so['path']

    def _trim_and_forward(self, method, path, *args, **kwargs):
        path = self._trim_filename(path)
        return getattr(super(DaskHDFileSystem, self), method)(path, *args, **kwargs)

    def open(self, path, mode='rb', **kwargs):
        return self._trim_and_forward('open', path, mode=mode, **kwargs)

    def mkdir(self, path):
        return self._trim_and_forward('mkdir', path)

    def exists(self, path):
        return self._trim_and_forward('exists', path)

    def isdir(self, path):
        return self._trim_and_forward('isdir', path)

    def isfile(self, path):
        return self._trim_and_forward('isfile', path)

    def ls(self, path, detail=False):
        return self._trim_and_forward('ls', path, detail=detail)

    def rm(self, path, recursive=False):
        return self._trim_and_forward('rm', path, recursive=recursive)

    def glob(self, path):
        return self._trim_and_forward('glob', path)

    def walk(self, path):
        return self._trim_and_forward('walk', path)

    def mkdirs(self, path):
        path = self._trim_filename(path)
        part = ['']
        for parts in path.split('/'):
            part.append(parts)
            try:
                super(DaskHDFileSystem, self).mkdir('/'.join(part))
            except Exception:
                pass

    def ukey(self, path):
        path = self._trim_filename(path)
        return tokenize(path, self.info(path)['last_mod'])

    def size(self, path):
        path = self._trim_filename(path)
        return self.info(path)['size']

    def get_block_locations(self, paths):
        offsets = []
        lengths = []
        machines = []
        for path in paths:
            path = self._trim_filename(path)
            out = super(DaskHDFileSystem, self).get_block_locations(path)
            offsets.append([o['offset'] for o in out])
            lengths.append([o['length'] for o in out])
            hosts = [[self._decode_hostname(h) for h in o['hosts']] for o in out]
            machines.append(hosts)
        return offsets, lengths, machines

    def _get_pyarrow_filesystem(self):
        from ._pyarrow import HDFSWrapper
        return HDFSWrapper(self)

    def _decode_hostname(self, host):
        # XXX this should be folded into the hdfs3 library
        if PY3 and isinstance(host, bytes):
            return host.decode()
        else:
            assert isinstance(host, str)
            return host


# Older versions of dask will try to load hdfs support from
# distributed.hdfs. We check this import first to then override
# it later.
try:
    import distributed.hdfs  # noqa
except ImportError:
    pass


core._filesystems['hdfs'] = DaskHDFileSystem
