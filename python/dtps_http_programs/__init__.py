import coloredlogs  # type: ignore

coloredlogs.install(level="DEBUG")  # type: ignore

from logging import getLogger, DEBUG

logger = getLogger(__name__)
logger.setLevel(DEBUG)

from .server_clock import *
from .dtps_stats import *
from .dtps_proxy import *
from .dtps_send_continuous import *
from .dtps_listen import *
