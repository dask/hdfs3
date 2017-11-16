from __future__ import absolute_import

from .conf import conf
from .core import HDFileSystem, HDFile
from .mapping import HDFSMap

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
