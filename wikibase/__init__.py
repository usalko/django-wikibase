from .version import get_version

VERSION = (0, 1, 1, 'alpha', 9)

__version__ = get_version(VERSION)

from . import expressions  # NOQA
