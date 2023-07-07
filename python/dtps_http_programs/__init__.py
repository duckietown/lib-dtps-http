import coloredlogs  # type: ignore

coloredlogs.install(level="DEBUG")  # type: ignore

from logging import getLogger, DEBUG

logger = getLogger(__name__)
logger.setLevel(DEBUG)

from .server_clock import *
from .dtps_stats import *
from .dtps_proxy import *
