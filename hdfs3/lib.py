# -*- coding: utf-8 -*-
"""
Low-level interface to libhdfs3
"""

import sys
import ctypes as ct
import os

PY3 = sys.version_info.major > 2
so_directory = '/usr/local/lib'
_lib = ct.cdll.LoadLibrary(os.sep.join([so_directory, 'libhdfs3.so']))

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
hdfsGetFileBlockLocations = _lib.hdfsGetFileBlockLocations
hdfsGetFileBlockLocations.argtypes = [ct.c_char_p]
hdfsGetFileBlockLocations.restype = ct.POINTER(BlockLocation)

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
hdfsListDirectory = _lib.hdfsListDirectory
hdfsListDirectory.restype = ct.POINTER(FileInfo)

class hdfsBuilder(ct.Structure):
    pass

class hdfsFile(ct.Structure):
    pass

class hdfsFS(ct.Structure):
    pass

hdfsGetLastError = _lib.hdfsGetLastError
hdfsGetLastError.argtypes = []
hdfsGetLastError.argtypes = [ct.c_char_p]
hdfsGetLastError.__doc__ = "Return error information of last failed operation."

hdfsFileIsOpenForRead = _lib.hdfsFileIsOpenForRead
hdfsFileIsOpenForRead.argtypes = [hdfsFile]
hdfsFileIsOpenForRead.restype = ct.c_bool
hdfsFileIsOpenForRead.__doc__ = "Determine if a file is open for read."

hdfsFileIsOpenForWrite = _lib.hdfsFileIsOpenForRead
hdfsFileIsOpenForWrite.argtypes = [hdfsFile]
hdfsFileIsOpenForWrite.restype = ct.c_bool
hdfsFileIsOpenForWrite.__doc__ = "Determine if a file is open for write."

hdfsConnectAsUser = _lib.hdfsConnectAsUser
hdfsConnectAsUser.argtypes = [ct.c_char_p, tPort, ct.c_char_p]
hdfsConnectAsUser.restype = hdfsFS
hdfsConnectAsUser.__doc__ = "Connect to a hdfs file system as a specific user"

hdfsConnectAsUserNewInstance = _lib.hdfsConnectAsUserNewInstance
hdfsConnectAsUserNewInstance.argtypes = [ct.c_char_p, tPort, ct.c_char_p]
hdfsConnectAsUserNewInstance.restype = hdfsFS
hdfsConnectAsUserNewInstance.__doc__ = "Connect to a hdfs file system as a specific user, new instance"

hdfsConnect = _lib.hdfsConnect
hdfsConnect.argtypes = [ct.c_char_p, tPort]
hdfsConnect.restype = hdfsFS
hdfsConnect.__doc__ = "Connect to a hdfs file system"

hdfsConnectNewInstance = _lib.hdfsConnectNewInstance
hdfsConnectNewInstance.argtypes = [ct.c_char_p, tPort]
hdfsConnectNewInstance.restype = hdfsFS
hdfsConnectNewInstance.__doc__ = "Connect to a hdfs file system"

hdfsBuilderConnect = _lib.hdfsBuilderConnect
hdfsBuilderConnect.argtypes = [hdfsBuilder]
hdfsBuilderConnect.restype = hdfsFS
hdfsBuilderConnect.__doc__ = "Connect to HDFS using the parameters defined by the builder."

hdfsNewBuilder = _lib.hdfsNewBuilder
hdfsNewBuilder.argtypes = []
hdfsNewBuilder.restype = hdfsBuilder
hdfsNewBuilder.__doc__ = "Create an HDFS builder."

hdfsBuilderSetForceNewInstance = _lib.hdfsBuilderSetForceNewInstance
hdfsBuilderSetForceNewInstance.argtypes = [hdfsBuilder]
hdfsBuilderSetForceNewInstance.restype = None
hdfsBuilderSetForceNewInstance.__doc__ = "Do nothing, we always create a new instance"

hdfsBuilderSetNameNode = _lib.hdfsBuilderSetNameNode
hdfsBuilderSetNameNode.argtypes = [hdfsBuilder, ct.c_char_p]
hdfsBuilderSetNameNode.restype = None
hdfsBuilderSetNameNode.__doc__ = "Set the HDFS NameNode to connect to."

hdfsBuilderSetNameNodePort = _lib.hdfsBuilderSetNameNodePort
hdfsBuilderSetNameNodePort.argtypes = [hdfsBuilder, tPort]
hdfsBuilderSetNameNodePort.restype = None
hdfsBuilderSetNameNodePort.__doc__ = "Set the port of the HDFS NameNode to connect to."

hdfsBuilderSetUserName = _lib.hdfsBuilderSetUserName
hdfsBuilderSetUserName.argtypes = [hdfsBuilder, ct.c_char_p]
hdfsBuilderSetUserName.restype = None
hdfsBuilderSetUserName.__doc__ = "Set the username to use when connecting to the HDFS cluster."

hdfsBuilderSetKerbTicketCachePath = _lib.hdfsBuilderSetKerbTicketCachePath
hdfsBuilderSetKerbTicketCachePath.argtypes = [hdfsBuilder, ct.c_char_p]
hdfsBuilderSetKerbTicketCachePath.restype = None
hdfsBuilderSetKerbTicketCachePath.__doc__ = "Set the path to the Kerberos ticket cache to use when connecting to the HDFS cluster."

hdfsBuilderSetToken = _lib.hdfsBuilderSetToken
hdfsBuilderSetToken.argtypes = [hdfsBuilder, ct.c_char_p]
hdfsBuilderSetToken.restype = None
hdfsBuilderSetToken.__doc__ = "Set the token used to authenticate"

hdfsFreeBuilder = _lib.hdfsFreeBuilder
hdfsFreeBuilder.argtypes = [hdfsBuilder]
hdfsFreeBuilder.restype = None
hdfsFreeBuilder.__doc__ = "Free an HDFS builder."

hdfsBuilderConfSetStr = _lib.hdfsBuilderConfSetStr
hdfsBuilderConfSetStr.argtypes = [hdfsBuilder, ct.c_char_p, ct.c_char_p]
hdfsBuilderConfSetStr.__doc__ = "Set a configuration string for an HdfsBuilder."

class HdfsFileInternalWrapper(ct.Structure):
    _fields_ = [('input', ct.c_bool),
                ('stream', ct.c_void_p)]
_lib.hdfsOpenFile.restype = ct.POINTER(HdfsFileInternalWrapper)


class HdfsFileSystemInternalWrapper(ct.Structure):
    # _fields_ = [('filesystem', ctypes.POINTER(FileSystem))]
    _fields_ = [('filesystem', ct.c_void_p)]  # TODO: expand this if needed
_lib.hdfsBuilderConnect.restype = ct.POINTER(HdfsFileSystemInternalWrapper)


"""
/**
 * Get a configuration string.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will be set to NULL if the
 *                 key isn't found.  You must free this string with
 *                 hdfsConfStrFree.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
int hdfsConfGetStr(const char * key, char ** val);

/**
 * Get a configuration integer.
 *
 * @param key      The key to find
 * @param val      (out param) The value.  This will NOT be changed if the
 *                 key isn't found.
 *
 * @return         0 on success; nonzero error code otherwise.
 *                 Failure to find the key is not an error.
 */
int hdfsConfGetInt(const char * key, int32_t * val);

/**
 * Free a configuration string found with hdfsConfGetStr.
 *
 * @param val      A configuration string obtained from hdfsConfGetStr
 */
void hdfsConfStrFree(char * val);

/**
 * hdfsDisconnect - Disconnect from the hdfs file system.
 * Disconnect from hdfs.
 * @param fs The configured filesystem handle.
 * @return Returns 0 on success, -1 on error.
 *         Even if there is an error, the resources associated with the
 *         hdfsFS will be freed.
 */
int hdfsDisconnect(hdfsFS fs);

/**
 * hdfsOpenFile - Open a hdfs file in given mode.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT),
 * O_WRONLY|O_APPEND and O_SYNC. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values.
 * @param replication Block replication - pass 0 if you want to use
 * the default configured values.
 * @param blocksize Size of block - pass 0 if you want to use the
 * default configured values.
 * @return Returns the handle to the open file or NULL on error.
 */
hdfsFile hdfsOpenFile(hdfsFS fs, const char * path, int flags, int bufferSize,
                      short replication, tOffset blocksize);

/**
 * hdfsCloseFile - Close an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 *         On error, errno will be set appropriately.
 *         If the hdfs file was valid, the memory associated with it will
 *         be freed at the end of this call, even if there was an I/O
 *         error.
 */
int hdfsCloseFile(hdfsFS fs, hdfsFile file);

/**
 * hdfsExists - Checks if a given path exsits on the filesystem
 * @param fs The configured filesystem handle.
 * @param path The path to look for
 * @return Returns 0 on success, -1 on error.
 */
int hdfsExists(hdfsFS fs, const char * path);

/**
 * hdfsSeek - Seek to given offset in file.
 * This works only for files opened in read-only mode.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);

/**
 * hdfsTell - Get the current offset in the file, in bytes.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Current offset, -1 on error.
 */
tOffset hdfsTell(hdfsFS fs, hdfsFile file);

/**
 * hdfsRead - Read data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return      On success, a positive number indicating how many bytes
 *              were read.
 *              On end-of-file, 0.
 *              On error, -1.  Errno will be set to the error code.
 *              Just like the POSIX read function, hdfsRead will return -1
 *              and set errno to EINTR if data is temporarily unavailable,
 *              but we are not yet at the end of the file.
 */
tSize hdfsRead(hdfsFS fs, hdfsFile file, void * buffer, tSize length);

/**
 * hdfsWrite - Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write.
 * @return Returns the number of bytes written, -1 on error.
 */
tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void * buffer, tSize length);

/**
 * hdfsWrite - Flush the data.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsFlush(hdfsFS fs, hdfsFile file);

/**
 * hdfsHFlush - Flush out the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 * @param fs configured filesystem handle
 * @param file file handle
 * @return 0 on success, -1 on error and sets errno
 */
int hdfsHFlush(hdfsFS fs, hdfsFile file);

/**
 * hdfsSync - Flush out and sync the data in client's user buffer. After the
 * return of this call, new readers will see the data.
 * @param fs configured filesystem handle
 * @param file file handle
 * @return 0 on success, -1 on error and sets errno
 */
int hdfsSync(hdfsFS fs, hdfsFile file);

/**
 * hdfsAvailable - Number of bytes that can be read from this
 * input stream without blocking.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns available bytes; -1 on error.
 */
int hdfsAvailable(hdfsFS fs, hdfsFile file);

/**
 * hdfsCopy - Copy file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsCopy(hdfsFS srcFS, const char * src, hdfsFS dstFS, const char * dst);

/**
 * hdfsMove - Move file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file.
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsMove(hdfsFS srcFS, const char * src, hdfsFS dstFS, const char * dst);

/**
 * hdfsDelete - Delete file.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param recursive if path is a directory and set to
 * non-zero, the directory is deleted else throws an exception. In
 * case of a file the recursive argument is irrelevant.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsDelete(hdfsFS fs, const char * path, int recursive);

/**
 * hdfsRename - Rename file.
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file.
 * @param newPath The path of the destination file.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsRename(hdfsFS fs, const char * oldPath, const char * newPath);

/**
 * hdfsGetWorkingDirectory - Get the current working directory for
 * the given filesystem.
 * @param fs The configured filesystem handle.
 * @param buffer The user-buffer to copy path of cwd into.
 * @param bufferSize The length of user-buffer.
 * @return Returns buffer, NULL on error.
 */
char * hdfsGetWorkingDirectory(hdfsFS fs, char * buffer, size_t bufferSize);

/**
 * hdfsSetWorkingDirectory - Set the working directory. All relative
 * paths will be resolved relative to it.
 * @param fs The configured filesystem handle.
 * @param path The path of the new 'cwd'.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsSetWorkingDirectory(hdfsFS fs, const char * path);

/**
 * hdfsCreateDirectory - Make the given file and all non-existent
 * parents into directories.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsCreateDirectory(hdfsFS fs, const char * path);

/**
 * hdfsSetReplication - Set the replication of the specified
 * file to the supplied value
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns 0 on success, -1 on error.
 */
int hdfsSetReplication(hdfsFS fs, const char * path, int16_t replication);

/**
 * hdfsListDirectory - Get list of files/directories for a given
 * directory-path. hdfsFreeFileInfo should be called to deallocate memory.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory.
 * @param numEntries Set to the number of files/directories in path.
 * @return Returns a dynamically-allocated array of hdfsFileInfo
 * objects; NULL on error.
 */
hdfsFileInfo * hdfsListDirectory(hdfsFS fs, const char * path, int * numEntries);

/**
 * hdfsGetPathInfo - Get information about a path as a (dynamically
 * allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
 * called when the pointer is no longer needed.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @return Returns a dynamically-allocated hdfsFileInfo object;
 * NULL on error.
 */
hdfsFileInfo * hdfsGetPathInfo(hdfsFS fs, const char * path);

/**
 * hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields)
 * @param infos The array of dynamically-allocated hdfsFileInfo
 * objects.
 * @param numEntries The size of the array.
 */
void hdfsFreeFileInfo(hdfsFileInfo * infos, int numEntries);

/**
 * hdfsGetHosts - Get hostnames where a particular block (determined by
 * pos & blocksize) of a file is stored. The last element in the array
 * is NULL. Due to replication, a single block could be present on
 * multiple hosts.
 * @param fs The configured filesystem handle.
 * @param path The path of the file.
 * @param start The start of the block.
 * @param length The length of the block.
 * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
 * NULL on error.
 */
char ***hdfsGetHosts(hdfsFS fs, const char *path, tOffset start,
                     tOffset length);

/**
 * hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
 * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
 * objects.
 * @param numEntries The size of the array.
 */
void hdfsFreeHosts(char ***blockHosts);

/**
 * hdfsGetDefaultBlockSize - Get the default blocksize.
 *
 * @param fs            The configured filesystem handle.
 * @deprecated          Use hdfsGetDefaultBlockSizeAtPath instead.
 *
 * @return              Returns the default blocksize, or -1 on error.
 */
tOffset hdfsGetDefaultBlockSize(hdfsFS fs);

/**
 * hdfsGetCapacity - Return the raw capacity of the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the raw-capacity; -1 on error.
 */
tOffset hdfsGetCapacity(hdfsFS fs);

/**
 * hdfsGetUsed - Return the total raw size of all files in the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the total-size; -1 on error.
 */
tOffset hdfsGetUsed(hdfsFS fs);

/**
 * Change the user and/or group of a file or directory.
 *
 * @param fs            The configured filesystem handle.
 * @param path          the path to the file or directory
 * @param owner         User string.  Set to NULL for 'no change'
 * @param group         Group string.  Set to NULL for 'no change'
 * @return              0 on success else -1
 */
int hdfsChown(hdfsFS fs, const char * path, const char * owner,
              const char * group);

/**
 * hdfsChmod
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mode the bitmask to set it to
 * @return 0 on success else -1
 */
int hdfsChmod(hdfsFS fs, const char * path, short mode);

/**
 * hdfsUtime
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mtime new modification time or -1 for no change
 * @param atime new access time or -1 for no change
 * @return 0 on success else -1
 */
int hdfsUtime(hdfsFS fs, const char * path, tTime mtime, tTime atime);

/**
 * hdfsTruncate - Truncate the file in the indicated path to the indicated size.
 * @param fs The configured filesystem handle.
 * @param path the path to the file.
 * @param pos the position the file will be truncated to.
 * @param shouldWait output value, true if and client does not need to wait for block recovery,
 * false if client needs to wait for block recovery.
 */
int hdfsTruncate(hdfsFS fs, const char * path, tOffset pos, int * shouldWait);

/**
 * Get a delegation token from namenode.
 * The token should be freed using hdfsFreeDelegationToken after canceling the token or token expired.
 *
 * @param fs The file system
 * @param renewer The user who will renew the token
 *
 * @return Return a delegation token, NULL on error.
 */
char * hdfsGetDelegationToken(hdfsFS fs, const char * renewer);

/**
 * Free a delegation token.
 *
 * @param token The token to be freed.
 */
void hdfsFreeDelegationToken(char * token);

/**
 * Renew a delegation token.
 *
 * @param fs The file system.
 * @param token The token to be renewed.
 *
 * @return the new expiration time
 */
int64_t hdfsRenewDelegationToken(hdfsFS fs, const char * token);

/**
 * Cancel a delegation token.
 *
 * @param fs The file system.
 * @param token The token to be canceled.
 *
 * @return return 0 on success, -1 on error.
 */
int hdfsCancelDelegationToken(hdfsFS fs, const char * token);

typedef struct Namenode {
    char * rpc_addr;    // namenode rpc address and port, such as "host:9000"
    char * http_addr;   // namenode http address and port, such as "host:50070"
} Namenode;

/**
 * If hdfs is configured with HA namenode, return all namenode informations as an array.
 * Else return NULL.
 *
 * Using configure file which is given by environment parameter LIBHDFS3_CONF
 * or "hdfs-client.xml" in working directory.
 *
 * @param nameservice hdfs name service id.
 * @param size output the size of returning array.
 *
 * @return return an array of all namenode information.
 */
Namenode * hdfsGetHANamenodes(const char * nameservice, int * size);

/**
 * If hdfs is configured with HA namenode, return all namenode informations as an array.
 * Else return NULL.
 *
 * @param conf the path of configure file.
 * @param nameservice hdfs name service id.
 * @param size output the size of returning array.
 *
 * @return return an array of all namenode information.
 */
Namenode * hdfsGetHANamenodesWithConfig(const char * conf, const char * nameservice, int * size);

/**
 * Free the array returned by hdfsGetConfiguredNamenodes()
 *
 * @param the array return by hdfsGetConfiguredNamenodes()
 */
void hdfsFreeNamenodeInformation(Namenode * namenodes, int size);

/**
 * Get an array containing hostnames, offset and size of portions of the given file.
 *
 * @param fs The file system
 * @param path The path to the file
 * @param start The start offset into the given file
 * @param length The length for which to get locations for
 * @param numOfBlock Output the number of elements in the returned array
 *
 * @return An array of BlockLocation struct.
 */
BlockLocation * hdfsGetFileBlockLocations(hdfsFS fs, const char * path,
        tOffset start, tOffset length, int * numOfBlock);

/**
 * Free the BlockLocation array returned by hdfsGetFileBlockLocations
 *
 * @param locations The array returned by hdfsGetFileBlockLocations
 * @param numOfBlock The number of elements in the locaitons
 */
void hdfsFreeFileBlockLocations(BlockLocation * locations, int numOfBlock);
"""