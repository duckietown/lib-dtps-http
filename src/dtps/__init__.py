"""DTPS."""

__version__ = "1.3.24"

from logging import DEBUG, getLogger

from dtps.config import *
from dtps.dtps_utils import *
from dtps.ergo_abstract import *
from dtps_http import RawData, TransformError

logger = getLogger(__name__)
logger.setLevel(DEBUG)

_ = RawData, TransformError
