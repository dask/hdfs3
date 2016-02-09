import os
import sys
import stat
from errno import ENOENT
from fuse import FUSE, Operations, FuseOSError
from hdfs3 import HDFileSystem


class HDFS(Operations):

    def __init__(self, path='.'):
        self.hdfs = HDFileSystem()
        self.root = path
    
    def getattr(self, path, fh=None):
        from pwd import getpwnam
        from grp import getgrnam
        info = self.hdfs.info(path)
        if not info :
            raise FuseOSError(ENOENT)
        data = {}
        user = getpwnam(info['owner'])
        group = getgrnam(info['group'])
        data['st_atime'] = float(info['last_access'])
        data['st_ctime'] = float(info['last_access'])
        data['st_uid'] = int(user.pw_uid)
        data['st_gid'] = group.gr_gid
        data['st_mtime'] = float(info['last_mod'])
        data['st_size'] = info['size']

        if info['kind'] == 'directory':
            data['st_mode'] = (stat.S_IFDIR | info['permissions']) 
            data['st_size'] = 4096 
            data['st_blksize'] = 4096
        else:
            data['st_mode'] = (stat.S_IFREG | info['permissions'])
            data['st_size'] = info['size']
            data['st_blksize'] = info['block_size']
            data['st_nlink'] = 1 

        return data
    

    def readdir(self, path, fh):
        return ['.', '..'] + [os.path.relpath(l['name'], path) for l in self.hdfs.ls(path)]

def main(mountpofloat, root):
    FUSE(HDFS(root), mountpofloat, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])

