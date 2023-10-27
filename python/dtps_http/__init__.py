__version__ = "0.0.0"

import coloredlogs  # type: ignore

coloredlogs.install(level="DEBUG")  # type: ignore

from logging import getLogger, DEBUG

logger = getLogger(__name__)
logger.setLevel(DEBUG)

from .client import *
from .constants import *
from .exceptions import *
from .server import *
from .server_start import *
from .structures import *
from .types import *
from .urls import *
from .utils import *
from .object_queue import *
