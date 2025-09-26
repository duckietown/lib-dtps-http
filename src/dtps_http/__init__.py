__version__ = "1.4.4"

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

# Import local optimizations to automatically enhance performance
try:
    from . import local_optimization
    from . import client_local_optimization
    logger.info("Local publishing optimizations loaded")
except ImportError as e:
    logger.debug(f"Could not load local optimizations: {e}")

# Optional WebSocket optimization
try:
    from . import websocket_optimization
    logger.info("WebSocket optimizations loaded")
except ImportError as e:
    logger.debug(f"Could not load WebSocket optimizations (aiohttp required): {e}")

getLogger("asyncio").setLevel(INFO)
getLogger("aiohttp.access").setLevel(WARNING)
getLogger("aiopubsub").setLevel(INFO)
getLogger("Hub").setLevel(INFO)
getLogger("urllib3.connectionpool").setLevel(WARNING)
