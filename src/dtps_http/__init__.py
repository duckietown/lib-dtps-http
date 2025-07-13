"""DTPS HTTP."""

__version__ = "1.3.24"

from logging import INFO, WARNING, getLogger

logger = getLogger(__name__)
logger.setLevel(INFO)

from dtps_http.client import *
from dtps_http.constants import *
from dtps_http.exceptions import *
from dtps_http.object_queue import *
from dtps_http.server import *
from dtps_http.server_start import *
from dtps_http.structures import *
from dtps_http.types_ import *
from dtps_http.types_of_source import *
from dtps_http.urls import *
from dtps_http.utils import *
from dtps_http.utils_every_once_in_a_while import *

asyncio_logger = getLogger("asyncio")
asyncio_logger.setLevel(INFO)
aiohttp_access_logger = getLogger("aiohttp.access")
aiohttp_access_logger.setLevel(WARNING)
aiopubsub_logger = getLogger("aiopubsub")
aiopubsub_logger.setLevel(INFO)
hub_logger = getLogger("Hub")
hub_logger.setLevel(INFO)
urllib3_connectionpool_logger = getLogger("urllib3.connectionpool")
urllib3_connectionpool_logger.setLevel(WARNING)
