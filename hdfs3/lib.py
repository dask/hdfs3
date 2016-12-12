# -*- coding: utf-8 -*-
"""
Low-level interface to libhdfs3
"""

import sys
import ctypes as ct


PY3 = sys.version_info.major > 2


try:
    _lib = ct.cdll.LoadLibrary('libhdfs3.so')
except OSError:
    try:
        import os
        env = os.path.dirname(os.path.dirname(sys.executable))
        _lib = ct.cdll.LoadLibrary(os.path.join(env, 'lib', 'libhdfs3.so'))
    except OSError:
        raise ImportError("Can not find the shared library: libhdfs3.so\n"
                "See installation instructions at "
                "http://hdfs3.readthedocs.io/en/latest/install.html")

tSize = ct.c_int32
tTime = ct.c_int64
tOffset = ct.c_int64
tPort = ct.c_uint16

class BlockLocation(ct.Structure):
    _fields_ = [('corrupt', ct.c_int),
                ('numOfNodes', ct.c_int),
                ('hosts', ct.POINTER(ct.c_char_p)),
                ('names', ct.POINTER(ct.c_char_p)),
                ('topologyPaths', ct.POINTER(ct.c_char_p)),
                ('length', ct.c_int64),
                ('offset', ct.c_int64)]
class FileInfo(ct.Structure):
    _fields_ = [('kind', ct.c_int8),
                ('name', ct.c_char_p),
                ('last_mod', ct.c_int64),  #time_t, could be 32bit
                ('size', ct.c_int64),
                ('replication', ct.c_short),
                ('block_size', ct.c_int64),
                ('owner', ct.c_char_p),
                ('group', ct.c_char_p),
                ('permissions', ct.c_short),  #view as octal
                ('last_access', ct.c_int64),  #time_t, could be 32bit
                ]
hdfsGetPathInfo = _lib.hdfsGetPathInfo
hdfsGetPathInfo.argtypes = [ct.c_char_p]
hdfsGetPathInfo.restype = ct.POINTER(FileInfo)
hdfsGetPathInfo.__doc__ = """Get information about a path as a (dynamically
allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
called when the pointer is no longer needed.

param fs The configured filesystem handle.
param path The path of the file.
return Returns a dynamically-allocated hdfsFileInfo object;
NULL on error."""

class hdfsBuilder(ct.Structure):
    pass

class hdfsFile(ct.Structure):
    _fields_ = [('input', ct.c_bool), ('stream', ct.c_void_p)]

class hdfsFS(ct.Structure):
    _fields_ = [('filesystem', ct.c_void_p)]  # TODO: expand this if needed

hdfsGetFileBlockLocations = _lib.hdfsGetFileBlockLocations
hdfsGetFileBlockLocations.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p,
                                      tOffset, tOffset, ct.POINTER(ct.c_int)]
hdfsGetFileBlockLocations.restype = ct.POINTER(BlockLocation)
hdfsGetFileBlockLocations.__doc__ = """Get an array containing hostnames, offset and size of portions of the given file.

param fs The file system
param path The path to the file
param start The start offset into the given file
param length The length for which to get locations for
param numOfBlock Output the number of elements in the returned array
return An array of BlockLocation struct."""


hdfsGetLastError = _lib.hdfsGetLastError
hdfsGetLastError.argtypes = []
hdfsGetLastError.restype = ct.c_char_p
hdfsGetLastError.__doc__ = """Return error information of last failed operation.

return A not NULL const string point of last error information.
Caller can only read this message and keep it unchanged. No need to free it.
If last operation finished successfully, the returned message is undefined."""

hdfsFileIsOpenForRead = _lib.hdfsFileIsOpenForRead
hdfsFileIsOpenForRead.argtypes = [ct.POINTER(hdfsFile)]
hdfsFileIsOpenForRead.restype = ct.c_bool
hdfsFileIsOpenForRead.__doc__ = """Determine if a file is open for read.

param file     The HDFS file
return         1 if the file is open for read; 0 otherwise"""

hdfsFileIsOpenForWrite = _lib.hdfsFileIsOpenForRead
hdfsFileIsOpenForWrite.argtypes = [ct.POINTER(hdfsFile)]
hdfsFileIsOpenForWrite.restype = ct.c_bool
hdfsFileIsOpenForWrite.__doc__ = """Determine if a file is open for write.

param file     The HDFS file
return         1 if the file is open for write; 0 otherwise"""

hdfsConnectAsUser = _lib.hdfsConnectAsUser
hdfsConnectAsUser.argtypes = [ct.c_char_p, tPort, ct.c_char_p]
hdfsConnectAsUser.restype = ct.POINTER(hdfsFS)
hdfsConnectAsUser.__doc__ = """Connect to a hdfs file system as a specific user

param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
param port The port on which the server is listening.
param user the user name (this is hadoop domain user). Or NULL is equivelant to hhdfsConnect(host, port)
return Returns a handle to the filesystem or NULL on error.
deprecated Use hdfsBuilderConnect instead."""

hdfsConnectAsUserNewInstance = _lib.hdfsConnectAsUserNewInstance
hdfsConnectAsUserNewInstance.argtypes = [ct.c_char_p, tPort, ct.c_char_p]
hdfsConnectAsUserNewInstance.restype = ct.POINTER(hdfsFS)
hdfsConnectAsUserNewInstance.__doc__ = "Connect to a hdfs file system as a specific user, new instance"

hdfsConnect = _lib.hdfsConnect
hdfsConnect.argtypes = [ct.c_char_p, tPort]
hdfsConnect.restype = ct.POINTER(hdfsFS)
hdfsConnect.__doc__ = """Connect to a hdfs file system

param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
param port The port on which the server is listening.
return Returns a handle to the filesystem or NULL on error.
deprecated Use hdfsBuilderConnect instead."""

hdfsConnectNewInstance = _lib.hdfsConnectNewInstance
hdfsConnectNewInstance.argtypes = [ct.c_char_p, tPort]
hdfsConnectNewInstance.restype = ct.POINTER(hdfsFS)
hdfsConnectNewInstance.__doc__ = "Connect to a hdfs file system"

hdfsBuilderConnect = _lib.hdfsBuilderConnect
hdfsBuilderConnect.argtypes = [ct.POINTER(hdfsBuilder)]
hdfsBuilderConnect.restype = ct.POINTER(hdfsFS)
hdfsBuilderConnect.__doc__ = """Connect to HDFS using the parameters defined by the builder.

The HDFS builder will be freed, whether or not the connection was
successful.

Every successful call to hdfsBuilderConnect should be matched with a call
to hdfsDisconnect, when the hdfsFS is no longer needed.

param bld    The HDFS builder
return       Returns a handle to the filesystem, or NULL on error.
"""

hdfsNewBuilder = _lib.hdfsNewBuilder
hdfsNewBuilder.argtypes = []
hdfsNewBuilder.restype = ct.POINTER(hdfsBuilder)
hdfsNewBuilder.__doc__ = "Create an HDFS builder."

hdfsBuilderSetForceNewInstance = _lib.hdfsBuilderSetForceNewInstance
hdfsBuilderSetForceNewInstance.argtypes = [ct.POINTER(hdfsBuilder)]
hdfsBuilderSetForceNewInstance.restype = None
hdfsBuilderSetForceNewInstance.__doc__ = "Do nothing, we always create a new instance"

hdfsBuilderSetNameNode = _lib.hdfsBuilderSetNameNode
hdfsBuilderSetNameNode.argtypes = [ct.POINTER(hdfsBuilder), ct.c_char_p]
hdfsBuilderSetNameNode.restype = None
hdfsBuilderSetNameNode.__doc__ = """Set the HDFS NameNode to connect to.

param bld  The HDFS builder
param nn   The NameNode to use.

             If the string given is 'default', the default NameNode
             configuration will be used (from the XML configuration files)

             If NULL is given, a LocalFileSystem will be created.

             If the string starts with a protocol type such as file:// or
             hdfs://, this protocol type will be used.  If not, the
             hdfs:// protocol type will be used.

             You may specify a NameNode port in the usual way by
             passing a string of the format hdfs://<hostname>:<port>.
             Alternately, you may set the port with
             hdfsBuilderSetNameNodePort.  However, you must not pass the
             port in two different ways."""

hdfsBuilderSetNameNodePort = _lib.hdfsBuilderSetNameNodePort
hdfsBuilderSetNameNodePort.argtypes = [ct.POINTER(hdfsBuilder), tPort]
hdfsBuilderSetNameNodePort.restype = None
hdfsBuilderSetNameNodePort.__doc__ = """Set the port of the HDFS NameNode to connect to.

param bld The HDFS builder
param port The port."""

hdfsBuilderSetUserName = _lib.hdfsBuilderSetUserName
hdfsBuilderSetUserName.argtypes = [ct.POINTER(hdfsBuilder), ct.c_char_p]
hdfsBuilderSetUserName.restype = None
hdfsBuilderSetUserName.__doc__ = """Set the username to use when connecting to the HDFS cluster.

param bld The HDFS builder
param userName The user name.  The string will be shallow-copied."""

hdfsBuilderSetKerbTicketCachePath = _lib.hdfsBuilderSetKerbTicketCachePath
hdfsBuilderSetKerbTicketCachePath.argtypes = [ct.POINTER(hdfsBuilder), ct.c_char_p]
hdfsBuilderSetKerbTicketCachePath.restype = None
hdfsBuilderSetKerbTicketCachePath.__doc__ = """Set the path to the Kerberos ticket cache to use when connecting to the HDFS cluster.

param bld The HDFS builder
param kerbTicketCachePath The Kerberos ticket cache path.  The string
                            will be shallow-copied."""

hdfsBuilderSetToken = _lib.hdfsBuilderSetToken
hdfsBuilderSetToken.argtypes = [ct.POINTER(hdfsBuilder), ct.c_char_p]
hdfsBuilderSetToken.restype = None
hdfsBuilderSetToken.__doc__ = """Set the token used to authenticate

param bld The HDFS builder
param token The token used to authenticate"""

hdfsFreeBuilder = _lib.hdfsFreeBuilder
hdfsFreeBuilder.argtypes = [ct.POINTER(hdfsBuilder)]
hdfsFreeBuilder.restype = None
hdfsFreeBuilder.__doc__ = "Free an HDFS builder."

hdfsBuilderConfSetStr = _lib.hdfsBuilderConfSetStr
hdfsBuilderConfSetStr.argtypes = [ct.POINTER(hdfsBuilder), ct.c_char_p, ct.c_char_p]
hdfsBuilderConfSetStr.__doc__ = """Set a configuration string for an HdfsBuilder.

param key      The key to set.
param val      The value, or NULL to set no value.
               This will be shallow-copied.  You are responsible for
               ensuring that it remains valid until the builder is
               freed.

return         0 on success; nonzero error code otherwise."""

hdfsDisconnect = _lib.hdfsDisconnect
hdfsDisconnect.argtypes = [ct.POINTER(hdfsFS)]
hdfsDisconnect.__doc__ = """Disconnect from the hdfs file system.

param fs The configured filesystem handle.
@return Returns 0 on success, -1 on error.
        Even if there is an error, the resources associated with the
        hdfsFS will be freed."""

hdfsOpenFile = _lib.hdfsOpenFile
hdfsOpenFile.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_int, ct.c_int,
                         ct.c_short, tOffset]
hdfsOpenFile.restype = ct.POINTER(hdfsFile)
hdfsOpenFile.__doc__ = """Open a hdfs file in given mode.

Open a hdfs file in given mode.
param fs The configured filesystem handle.
param path The full path to the file.
param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
param bufferSize Size of buffer for read/write - pass 0 if you want
 to use the default configured values.
param replication Block replication - pass 0 if you want to use
 the default configured values.
param blocksize Size of block - pass 0 if you want to use the
 default configured values.
return Returns the handle to the open file or NULL on error."""

hdfsCloseFile = _lib.hdfsCloseFile
hdfsCloseFile.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile)]
hdfsCloseFile.__doc__ = """Close an open file.

param fs The configured filesystem handle.
param file The file handle.
return Returns 0 on success, -1 on error.
       On error, errno will be set appropriately.
       If the hdfs file was valid, the memory associated with it will
       be freed at the end of this call, even if there was an I/O
       error."""

hdfsExists = _lib.hdfsExists
hdfsExists.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsExists.__doc__ = """Checks if a given path exsits on the filesystem

param fs The configured filesystem handle.
param path The path to look for
return Returns 0 on success, -1 on error."""

hdfsSeek = _lib.hdfsSeek
hdfsSeek.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile), tOffset]
hdfsSeek.__doc__ = """Seek to given offset in file.
This works only for files opened in read-only mode.

param fs The configured filesystem handle.
param file The file handle.
param desiredPos Offset into the file to seek into.
return Returns 0 on success, -1 on error."""

hdfsTell = _lib.hdfsTell
hdfsTell.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile)]
hdfsTell.restype = tOffset
hdfsTell.__doc__ = """Get the current offset in the file, in bytes.

param fs The configured filesystem handle.
param file The file handle.
return Current offset, -1 on error."""

hdfsRead = _lib.hdfsRead
hdfsRead.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile), ct.c_void_p,
                     tSize]
hdfsRead.restype = tSize
hdfsRead.__doc__ = """Read data from an open file.

param fs The configured filesystem handle.
param file The file handle.
param buffer The buffer to copy read bytes into.
param length The length of the buffer.
return      On success, a positive number indicating how many bytes
            were read.
            On end-of-file, 0.
            On error, -1.  Errno will be set to the error code.
            Just like the POSIX read function, hdfsRead will return -1
            and set errno to EINTR if data is temporarily unavailable,
            but we are not yet at the end of the file."""

hdfsWrite = _lib.hdfsWrite
hdfsWrite.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile), ct.c_void_p,
                     tSize]
hdfsWrite.restype = tSize
hdfsWrite.__doc__ = """Write data into an open file.

param fs The configured filesystem handle.
param file The file handle.
param buffer The data.
param length The no. of bytes to write.
return Returns the number of bytes written, -1 on error."""

hdfsHFlush = _lib.hdfsHFlush
hdfsHFlush.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile)]
hdfsHFlush.__doc__ = """Flush the data.

param fs The configured filesystem handle.
param file The file handle.
return Returns 0 on success, -1 on error."""

hdfsSync = _lib.hdfsSync
hdfsSync.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile)]
hdfsSync.__doc__ = """Flush out and sync the data in client's user buffer.
After the return of this call, new readers will see the data.

param fs configured filesystem handle
param file file handle
return 0 on success, -1 on error and sets errno"""

hdfsAvailable = _lib.hdfsAvailable
hdfsAvailable.argtypes = [ct.POINTER(hdfsFS), ct.POINTER(hdfsFile)]
hdfsAvailable.__doc__ = """Number of bytes that can be read without blocking.

param fs The configured filesystem handle.
param file The file handle.
return Returns available bytes; -1 on error."""

hdfsCopy = _lib.hdfsCopy
hdfsCopy.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p,
                     ct.POINTER(hdfsFS), ct.c_char_p]
hdfsCopy.__doc__ = """Copy file from one filesystem to another.

param srcFS The handle to source filesystem.
param src The path of source file.
param dstFS The handle to destination filesystem.
param dst The path of destination file.
return Returns 0 on success, -1 on error."""

hdfsMove = _lib.hdfsMove
hdfsMove.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p,
                     ct.POINTER(hdfsFS), ct.c_char_p]
hdfsMove.__doc__ = """Move file from one filesystem to another.

param srcFS The handle to source filesystem.
param src The path of source file.
param dstFS The handle to destination filesystem.
param dst The path of destination file.
return Returns 0 on success, -1 on error."""

hdfsDelete = _lib.hdfsDelete
hdfsDelete.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_int]
hdfsDelete.__doc__ = """Delete file.

param fs The configured filesystem handle.
param path The path of the file.
param recursive if path is a directory and set to
 non-zero, the directory is deleted else throws an exception. In
 case of a file the recursive argument is irrelevant.
return Returns 0 on success, -1 on error."""

hdfsRename = _lib.hdfsRename
hdfsRename.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_char_p]
hdfsRename.__doc__ = """Rename file.

param fs The configured filesystem handle.
param oldPath The path of the source file.
param newPath The path of the destination file.
return Returns 0 on success, -1 on error.
"""

hdfsGetWorkingDirectory = _lib.hdfsGetWorkingDirectory
hdfsGetWorkingDirectory.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, tSize]
hdfsGetWorkingDirectory.restype = ct.c_char_p
hdfsGetWorkingDirectory.__doc__ = """Get the current working directory for
the given filesystem.

param fs The configured filesystem handle.
param buffer The user-buffer to copy path of cwd into.
param bufferSize The length of user-buffer.
return Returns buffer, NULL on error.
"""

hdfsSetWorkingDirectory = _lib.hdfsSetWorkingDirectory
hdfsSetWorkingDirectory.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsSetWorkingDirectory.__doc__ = """Set the working directory. All relative
paths will be resolved relative to it.

param fs The configured filesystem handle.
param path The path of the new 'cwd'.
return Returns 0 on success, -1 on error."""

hdfsCreateDirectory = _lib.hdfsCreateDirectory
hdfsCreateDirectory.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsCreateDirectory.__doc__ = """Make the given file and all non-existent
parents into directories.

param fs The configured filesystem handle.
param path The path of the directory.
return Returns 0 on success, -1 on error."""

hdfsSetReplication = _lib.hdfsSetReplication
hdfsSetReplication.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_int16]
hdfsSetReplication.__doc__ = """Set the replication of the specified
file to the supplied value

param fs The configured filesystem handle.
param path The path of the file.
return Returns 0 on success, -1 on error."""

hdfsListDirectory = _lib.hdfsListDirectory
hdfsListDirectory.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p,
                              ct.POINTER(ct.c_int)]
hdfsListDirectory.restype = ct.POINTER(FileInfo)
hdfsListDirectory.__doc__ = """Get list of files/directories for a given
directory-path. hdfsFreeFileInfo should be called to deallocate memory.

param fs The configured filesystem handle.
param path The path of the directory.
param numEntries Set to the number of files/directories in path.
return Returns a dynamically-allocated array of hdfsFileInfo
objects; NULL on error."""

hdfsGetPathInfo = _lib.hdfsGetPathInfo
hdfsGetPathInfo.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsGetPathInfo.restype = ct.POINTER(FileInfo)
hdfsGetPathInfo.__doc__ = """Get information about a path as a (dynamically
allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
called when the pointer is no longer needed.

param fs The configured filesystem handle.
param path The path of the file.
return Returns a dynamically-allocated hdfsFileInfo object;
NULL on error."""

hdfsFreeFileInfo = _lib.hdfsFreeFileInfo
hdfsFreeFileInfo.argtypes = [ct.POINTER(FileInfo), ct.c_int]
hdfsFreeFileInfo.restype = None
hdfsFreeFileInfo.__doc__ = """Free up the hdfsFileInfo array (including fields)
param infos The array of dynamically-allocated hdfsFileInfo
objects.

param numEntries The size of the arr"""

hdfsGetDefaultBlockSize = _lib.hdfsGetDefaultBlockSize
hdfsGetDefaultBlockSize.argtypes = [ct.POINTER(hdfsFS)]
hdfsGetDefaultBlockSize.restype = tOffset
hdfsGetDefaultBlockSize.__doc__ = """Get the default blocksize.

param fs            The configured filesystem handle.
deprecated          Use hdfsGetDefaultBlockSizeAtPath instead.
return              Returns the default blocksize, or -1 on error."""

hdfsGetCapacity = _lib.hdfsGetCapacity
hdfsGetCapacity.argtypes = [ct.POINTER(hdfsFS)]
hdfsGetCapacity.restype = tOffset
hdfsGetCapacity.__doc__ = """Return the raw capacity of the filesystem.

param fs The configured filesystem handle.
return Returns the raw-capacity; -1 on error."""

hdfsGetUsed = _lib.hdfsGetUsed
hdfsGetUsed.argtypes = [ct.POINTER(hdfsFS)]
hdfsGetUsed.restype = tOffset
hdfsGetUsed.__doc__ = """Return the total raw size of all files in the filesystem.

param fs The configured filesystem handle.
return Returns the total-size; -1 on error."""

hdfsChown = _lib.hdfsChown
hdfsChown.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_char_p, ct.c_char_p]
hdfsChown.__doc__ = """Change the user and/or group of a file or directory.

param fs            The configured filesystem handle.
param path          the path to the file or directory
param owner         User string.  Set to NULL for 'no change'
param group         Group string.  Set to NULL for 'no change'
return              0 on success else -1"""

hdfsChmod = _lib.hdfsChmod
hdfsChmod.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, ct.c_short]
hdfsChmod.__doc__ = """param fs The configured filesystem handle.

param path the path to the file or directory
param mode the bitmask to set it to
return 0 on success else -1"""

hdfsUtime = _lib.hdfsUtime
hdfsUtime.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, tTime, tTime]
hdfsUtime.__doc__ = """@param fs The configured filesystem handle.

param path the path to the file or directory
param mtime new modification time or -1 for no change
param atime new access time or -1 for no change
return 0 on success else -1"""

hdfsTruncate = _lib.hdfsTruncate
hdfsTruncate.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p, tOffset, ct.c_int]
hdfsTruncate.__doc__ = """Truncate the file in the indicated path to the indicated size.

param fs The configured filesystem handle.
param path the path to the file.
param pos the position the file will be truncated to.
param shouldWait output value, true if and client does not need to wait for block recovery,
false if client needs to wait for block recovery."""

hdfsGetDelegationToken = _lib.hdfsGetDelegationToken
hdfsGetDelegationToken.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsGetDelegationToken.restype = ct.c_char_p
hdfsGetDelegationToken.__doc__ = """Get a delegation token from namenode.
The token should be freed using hdfsFreeDelegationToken after canceling the token or token expired.

param fs The file system
param renewer The user who will renew the token
return Return a delegation token, NULL on error."""

hdfsFreeDelegationToken = _lib.hdfsFreeDelegationToken
hdfsFreeDelegationToken.argtypes = [ct.c_char_p]
hdfsFreeDelegationToken.restype = None
hdfsFreeDelegationToken.__doc__ = """Free a delegation token.

param token The token to be freed."""

hdfsRenewDelegationToken = _lib.hdfsRenewDelegationToken
hdfsRenewDelegationToken.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsRenewDelegationToken.restype = ct.c_int64
hdfsRenewDelegationToken.__doc__ = """Renew a delegation token.

param fs The file system.
param token The token to be renewed.
return the new expiration time"""

hdfsCancelDelegationToken = _lib.hdfsCancelDelegationToken
hdfsCancelDelegationToken.argtypes = [ct.POINTER(hdfsFS), ct.c_char_p]
hdfsCancelDelegationToken.__doc__ = """Cancel a delegation token.

param fs The file system.
param token The token to be canceled.
return return 0 on success, -1 on error."""

hdfsFreeFileBlockLocations = _lib.hdfsFreeFileBlockLocations
hdfsFreeFileBlockLocations.argtypes = [ct.POINTER(BlockLocation), ct.c_int]
hdfsFreeFileBlockLocations.restype = None
hdfsFreeFileBlockLocations.__doc__ = """Free the BlockLocation array returned by hdfsGetFileBlockLocations

param locations The array returned by hdfsGetFileBlockLocations
param numOfBlock The number of elements in the locations"""
