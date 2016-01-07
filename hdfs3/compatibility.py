import sys

if sys.version_info < (3,):
    FileNotFoundError = IOError
    PermissionError = IOError
else:
    PermissionError = PermissionError
    FileNotFoundError = FileNotFoundError
