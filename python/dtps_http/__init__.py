__version__ = "1.0.8"

import coloredlogs  # type: ignore

coloredlogs.install(level="DEBUG")  # type: ignore

from logging import getLogger, INFO, WARNING

logger = getLogger(__name__)
logger.setLevel(INFO)

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

getLogger("asyncio").setLevel(INFO)
getLogger('aiohttp.access').setLevel(WARNING)
getLogger("aiopubsub").setLevel(INFO)
getLogger("Hub").setLevel(INFO)
getLogger("urllib3.connectionpool").setLevel(WARNING)
