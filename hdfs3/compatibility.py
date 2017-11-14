# flake8: noqa
import sys

PY3 = sys.version_info.major == 3
PY2 = not PY3


if PY2:
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
