__version__ = "1.4.0"

from logging import DEBUG, getLogger, INFO, WARNING

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
from .types_of_source import *
from .utils_every_once_in_a_while import *

getLogger("asyncio").setLevel(INFO)
getLogger("aiohttp.access").setLevel(WARNING)
getLogger("aiopubsub").setLevel(INFO)
getLogger("Hub").setLevel(INFO)
getLogger("urllib3.connectionpool").setLevel(WARNING)
