from . import redb as _extension
from .redb import *
from .redb import bool, bytes, open, str

__doc__ = _extension.__doc__
__version__ = _extension.__version__
# Left out, so that `from redb import *` does not shadow the builtins of the same names
__all__ = [name for name in _extension.__all__ if name not in {"bool", "bytes", "open", "str"}]
