
from collections import MutableMapping

class HDFSMap(MutableMapping):
    """Wrap an S3FileSystem as a mutable wrapping.

    The keys of the mapping become files under the given root, and the
    values (which must be bytes) the contents of those files.

    Parameters
    ----------
    s3 : S3FileSystem
    root : string
        prefix for all the files (perhaps justa  bucket name
    check : bool (=True)
        performs a touch at the location, to check writeability.

    Examples
    --------
    >>> s3 = hdfs3.HDFileSystem() # doctest: +SKIP
    >>> mw = HDFSMap(s3, '/mapstore/') # doctest: +SKIP
    >>> mw['loc1'] = b'Hello World' # doctest: +SKIP
    >>> list(mw.keys()) # doctest: +SKIP
    ['loc1']
    >>> mw['loc1'] # doctest: +SKIP
    b'Hello World'
    """

    def __init__(self, hdfs, root, check=False):
        self.hdfs = hdfs
        self.root = root
        if not hdfs.exists(root):
            hdfs.mkdir(root)
        if check:
            hdfs.ls(root)
            hdfs.touch(root+'/a')
            hdfs.rm(root+'/a')

    def clear(self):
        """Remove all keys below root - empties out mapping
        """
        self.hdfs.rm(self.root, recursive=True)
        self.hdfs.mkdir(self.root)

    def _key_to_str(self, key):
        if isinstance(key, (tuple, list)):
            key = str(tuple(key))
        else:
            key = str(key)
        if '/' in key:
            raise ValueError("Keys containing '/' disallowed")
        return '/'.join([self.root, key])

    def __getitem__(self, key):
        key = self._key_to_str(key)
        try:
            with self.hdfs.open(key, 'rb') as f:
                result = f.read()
        except (IOError, OSError):
            raise KeyError(key)
        return result

    def __setitem__(self, key, value):
        key = self._key_to_str(key)
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        with self.hdfs.open(key, 'wb') as f:
            f.write(value)

    def keys(self):
        l = len(self.root) + 1
        return (fn[l:] for fn in self.hdfs.walk(self.root) if len(fn) > l)

    def __iter__(self):
        return self.keys()

    def __delitem__(self, key):
        self.hdfs.rm(self._key_to_str(key))

    def __contains__(self, key):
        return self.hdfs.exists(self._key_to_str(key))

    def __len__(self):
        return sum(1 for _ in self.keys())
