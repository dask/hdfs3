import sys

if sys.version_info < (3,):

    class ConnectionError(OSError):
        """Connection to HDFS failed."""

    FileNotFoundError = IOError
    PermissionError = IOError
    from urlparse import urlparse
    unicode = unicode
    bytes = str
else:
    ConnectionError = ConnectionError
    PermissionError = PermissionError
    FileNotFoundError = FileNotFoundError
    from urllib.parse import urlparse
    unicode = str
    bytes = bytes
