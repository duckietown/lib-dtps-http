from logging import getLogger, DEBUG

logger = getLogger(__name__)
logger.setLevel(DEBUG)

from .config import *
from .ergo_ui import *

from dtps_http import RawData

_ = RawData
