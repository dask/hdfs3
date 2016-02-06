import sys

if sys.version_info < (3,):
    FileNotFoundError = IOError
    PermissionError = IOError
    from urlparse import urlparse
    unicode = unicode
    bytes = str
else:
    PermissionError = PermissionError
    FileNotFoundError = FileNotFoundError
    from urllib.parse import urlparse
    unicode = str
    bytes = bytes
