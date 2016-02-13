from __future__ import print_function
import os
import sys
import stat
from errno import ENOENT, EIO
from fuse import FUSE, Operations, FuseOSError
from hdfs3 import HDFileSystem
from hdfs3.core import FileNotFoundError


class HDFS(Operations):

    def __init__(self, path='.'):
        self.hdfs = HDFileSystem()
        self.root = path

    def getattr(self, path, fh=None):
        from pwd import getpwnam
        from grp import getgrnam
        try:
            info = self.hdfs.info(path)
        except FileNotFoundError:
            raise FuseOSError(ENOENT)
        data = {}
        try:
            user = getpwnam(info['owner'])
            group = getgrnam(info['group'])
        except KeyError:
            user = getpwnam('root')
            group = getgrnam('root')
        data['st_atime'] = float(info['last_access'])
        data['st_ctime'] = float(info['last_access'])
        data['st_uid'] = int(user.pw_uid)
        data['st_gid'] = group.gr_gid
        data['st_mtime'] = float(info['last_mod'])
        data['st_size'] = info['size']

        if info['kind'] == 'directory':
            data['st_mode'] = (stat.S_IFDIR | info['permissions'])
            data['st_size'] = 0
            data['st_blksize'] = 0
        else:
            data['st_mode'] = (stat.S_IFREG | info['permissions'])
            data['st_size'] = info['size']
            data['st_blksize'] = info['block_size']
            data['st_nlink'] = 1

        return data

    def readdir(self, path, fh):
        return ['.', '..'] + [os.path.relpath(l['name'], path)
                              for l in self.hdfs.ls(path)]

    def mkdir(self, path, mode):
        self.hdfs.mkdir(path)

    def rmdir(self, path):
        self.hdfs.rm(path, False)

    def read(self, path, size, offset, fh):
        if offset == 0:
            with self.hdfs.open(path, 'rb') as f:
                f.seek(offset)
                return f.read(size)

    def write(self, path, data, offset, fh):
        if offset == 0:
            with self.hdfs.open(path, 'wb') as f:
                f.write(data)
                return len(data)

    def create(self, path, flags):
        self.hdfs.touch(path)
        return 0

    def open(self, path, flags):
        if flags % 2 == 0:
            return 0
        return 1

    def truncate(self, path, length, fh=None):
        if length == 0:
            self.hdfs.open(path, 'wb')

    def unlink(self, path):
        try:
            self.hdfs.rm(path, False)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)

    def release(self, path, fh):
        return 0

    def chmod(self, path, mode):
        try:
            self.hdfs.chmod(path, mode)
        except (IOError, FileNotFoundError):
            raise FuseOSError(EIO)


def main(mountpofloat, root):
    FUSE(HDFS(root), mountpofloat, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
