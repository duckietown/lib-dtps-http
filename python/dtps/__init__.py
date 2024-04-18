__version__ = "1.2.1"

from logging import DEBUG, getLogger

logger = getLogger(__name__)
logger.setLevel(DEBUG)

from .config import *
from .ergo_ui import *

from dtps_http import RawData, TransformError

_ = RawData, TransformError
