"""DTPS HTTP programs."""

__version__ = "1.3.24"

from logging import DEBUG, getLogger

from dtps_http_programs.dtps_listen import *
from dtps_http_programs.dtps_proxy import *
from dtps_http_programs.dtps_send_continuous import *
from dtps_http_programs.dtps_stats import *
from dtps_http_programs.server_clock import *
from dtps_http_programs.together import *

logger = getLogger(__name__)
logger.setLevel(DEBUG)
