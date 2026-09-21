import os
import struct

from .types import ContentType, TopicNameV

__all__ = [
    "CONTENT_TYPE_DTPS_DATAREADY_CBOR",
    "CONTENT_TYPE_DTPS_INDEX",
    "CONTENT_TYPE_DTPS_INDEX_CBOR",
    "CONTENT_TYPE_PATCH_CBOR",
    "CONTENT_TYPE_PATCH_JSON",
    "CONTENT_TYPE_PATCH_YAML",
    "CONTENT_TYPE_TOPIC_HISTORY_CBOR",
    "DEFAULT_DATA_AVAILABILITY_TIMEOUT",
    "DEFAULT_MAX_HISTORY",
    "DEFAULT_CALLBACK_QUEUE_SIZE",
    "ENV_MASK_ORIGIN",
    "EVENTS_SUFFIX",
    "HEADER_CONTENT_LOCATION",
    "HEADER_DATA_ORIGIN_NODE_ID",
    "HEADER_DATA_UNIQUE_ID",
    "HEADER_LINK_BENCHMARK",
    "HEADER_MAX_FREQUENCY",
    "HEADER_NODE_ID",
    "HEADER_NODE_PASSED_THROUGH",
    "HEADER_NO_AVAIL",
    "HEADER_NO_CACHE",
    "HTTP_TIMEOUT",
    "MIME_CBOR",
    "MIME_HTML",
    "MIME_JPEG",
    "MIME_JSON",
    "MIME_OCTET",
    "MIME_TEXT",
    "MIME_YAML",
    "REL_CONNECTIONS",
    "REL_EVENTS_DATA",
    "REL_EVENTS_NODATA",
    "REL_HISTORY",
    "REL_HISTORY",
    "REL_META",
    "REL_PROXIED",
    "REL_STREAM_PUSH",
    "REL_STREAM_PUSH_SUFFIX",
    "REL_URL_HISTORY",
    "REL_URL_META",
    "SHM_CHANNEL_DIRECTORY_MODE",
    "SHM_CHANNEL_FILE_MODE",
    "SHM_DEFAULT_CAPACITY",
    "SHM_ERROR_CAPACITY",
    "SHM_ERROR_CAPACITY_EXCEEDS_MAXIMUM",
    "SHM_ERROR_CHANNEL_DIRECTORY_NOT_DIRECTORY",
    "SHM_ERROR_CHANNEL_DIRECTORY_NOT_OWNER",
    "SHM_ERROR_CHANNEL_DIRECTORY_SYMLINK",
    "SHM_ERROR_CHANNEL_DIRECTORY_WORLD_WRITABLE",
    "SHM_ERROR_CHANNEL_PATH_HAS_MULTIPLE_LINKS",
    "SHM_ERROR_CHANNEL_PATH_NOT_REGULAR_FILE",
    "SHM_ERROR_CLOSE_LOCK_FILE",
    "SHM_ERROR_CLOSE_MAP",
    "SHM_ERROR_CLOSE_SIGNAL_FIFO",
    "SHM_ERROR_HANDLE_PAYLOAD",
    "SHM_ERROR_HEADER_MAGIC",
    "SHM_ERROR_HEADER_SIZE",
    "SHM_ERROR_HEADER_UNPACK",
    "SHM_ERROR_PAYLOAD_LENGTH",
    "SHM_ERROR_READ_PAYLOAD",
    "SHM_ERROR_READER_LOCK_NOT_OPEN",
    "SHM_ERROR_READER_NOT_OPEN",
    "SHM_ERROR_READER_REMAP_FAILED",
    "SHM_ERROR_READER_STOPPING",
    "SHM_ERROR_SIGNAL_FIFO_ACCESSIBLE_BY_OTHERS",
    "SHM_ERROR_SIGNAL_FIFO_NOT_OWNER",
    "SHM_ERROR_SIGNAL_PATH_NOT_FIFO",
    "SHM_ERROR_SIGNAL_POLL_FAILED",
    "SHM_ERROR_SIGNAL_READ_FAILED",
    "SHM_ERROR_UNSUPPORTED_VERSION",
    "SHM_ERROR_UNSUPPORTED_PLATFORM",
    "SHM_ERROR_WRITER_LOCK_NOT_OPEN",
    "SHM_ERROR_WRITER_MAP_NOT_OPEN",
    "SHM_ERROR_WRITER_NOT_OPEN",
    "SHM_FIFO_FULL_WARNING",
    "SHM_HEADER_FORMAT",
    "SHM_HEADER_SIZE",
    "SHM_LOCK_SUFFIX",
    "SHM_MAGIC",
    "SHM_MAX_CAPACITY",
    "SHM_SIGNAL_DRAIN_READS",
    "SHM_SIGNAL_DRAIN_READ_SIZE",
    "SHM_SIGNAL_FORMAT",
    "SHM_SIGNAL_POLL_TIMEOUT_SECONDS",
    "SHM_SIGNAL_SUFFIX",
    "SHM_STOP_JOIN_TIMEOUT_SECONDS",
    "SHM_VERSION",
    "SHM_WARNING_READER_STILL_STOPPING",
    "SHM_WARNING_RESET_HEADER",
    "TOPIC_AVAILABILITY",
    "TOPIC_CLOCK",
    "TOPIC_CONNECTIONS",
    "TOPIC_LIST",
    "TOPIC_LOGS",
    "TOPIC_PROXIED",
    "TOPIC_STATE_NOTIFICATION",
    "TOPIC_STATE_SUMMARY",
]

HEADER_NO_CACHE = {
    "Cache-Control": "no-store, must-revalidate, max-age=0, post-check=0, pre-check=0",
    "Pragma": "no-cache",
    "Expires": "0",
}
HEADER_NODE_ID = "X-DTPS-Node-ID"
HEADER_NODE_PASSED_THROUGH = "X-DTPS-Node-ID-Passed-Through"
HEADER_LINK_BENCHMARK = "X-DTPS-link-benchmark"
HEADER_DATA_UNIQUE_ID = "X-DTPS-data-unique-id"
HEADER_DATA_ORIGIN_NODE_ID = "X-DTPS-data-origin-node"
HEADER_MAX_FREQUENCY = "X-DTPS-Max-Frequency"

TOPIC_LIST = TopicNameV.from_dash_sep("dtps/topic_list")
TOPIC_CLOCK = TopicNameV.from_dash_sep("dtps/clock")
TOPIC_LOGS = TopicNameV.from_dash_sep("dtps/logs")
TOPIC_AVAILABILITY = TopicNameV.from_dash_sep("dtps/availability")
TOPIC_STATE_SUMMARY = TopicNameV.from_dash_sep("dtps/state")
TOPIC_CONNECTIONS = TopicNameV.from_dash_sep("dtps/connections")
TOPIC_STATE_NOTIFICATION = TopicNameV.from_dash_sep("dtps/states-notification")
TOPIC_PROXIED = TopicNameV.from_dash_sep("dtps/proxied")

CONTENT_TYPE_DTPS_INDEX = ContentType("application/vnd.dt.dtps-index")
CONTENT_TYPE_DTPS_INDEX_CBOR = ContentType("application/vnd.dt.dtps-index+cbor")
CONTENT_TYPE_DTPS_DATAREADY_CBOR = ContentType("application/vnd.dt.dtps-dataready+cbor")
CONTENT_TYPE_TOPIC_HISTORY_CBOR = ContentType("application/vnd.dt.dtps-history+cbor")
CONTENT_TYPE_PATCH_JSON = ContentType("application/json-patch+json")
CONTENT_TYPE_PATCH_YAML = ContentType("application/json-patch+yaml")
CONTENT_TYPE_PATCH_CBOR = ContentType("application/json-patch+cbor")

HEADER_NO_AVAIL = "X-dtps-debug-Content-Location-Not-Available"
HEADER_CONTENT_LOCATION = "Content-Location"

REL_EVENTS_NODATA = "dtps-events"
REL_EVENTS_DATA = "dtps-events-inline-data"
REL_STREAM_PUSH = "dtps-events-push"
REL_META = "dtps-meta"
REL_HISTORY = "dtps-history"
REL_CONNECTIONS = "dtps-connections"
REL_PROXIED = "dtps-proxied"

EVENTS_SUFFIX = ":events"
REL_STREAM_PUSH_SUFFIX = ":push"
REL_URL_META = ":meta"
REL_URL_HISTORY = ":history"

MIME_CBOR = ContentType("application/cbor")
MIME_JSON = ContentType("application/json")
MIME_YAML = ContentType("application/yaml")
MIME_TEXT = ContentType("text/plain")
MIME_HTML = ContentType("text/html")
MIME_JPEG = ContentType("image/jpeg")

MIME_OCTET = ContentType("application/octet-stream")

ENV_MASK_ORIGIN = "DTPS_HTTP_MASK_ORIGIN"

HTTP_TIMEOUT: float = float(os.environ.get("DTPS_HTTP_TIMEOUT", "10"))

DEFAULT_MAX_HISTORY: int = 10
DEFAULT_DATA_AVAILABILITY_TIMEOUT: float = float(os.environ.get("DTPS_DATA_AVAILABILITY_TIMEOUT", "60"))

DEFAULT_CALLBACK_QUEUE_SIZE: int = 10

SHM_CHANNEL_DIRECTORY_MODE = 0o700
SHM_CHANNEL_FILE_MODE = 0o600
SHM_DEFAULT_CAPACITY = 1024 * 1024
SHM_MAX_CAPACITY = 64 * 1024 * 1024
SHM_HEADER_FORMAT = "<4sIII"
SHM_HEADER_SIZE = struct.calcsize(SHM_HEADER_FORMAT)
SHM_LOCK_SUFFIX = ".lock"
SHM_MAGIC = b"DTCM"
SHM_SIGNAL_FORMAT = "<Q"
SHM_SIGNAL_DRAIN_READ_SIZE = 4096
SHM_SIGNAL_DRAIN_READS = 16
SHM_SIGNAL_SUFFIX = ".p2c"
SHM_SIGNAL_POLL_TIMEOUT_SECONDS = 0.1
SHM_STOP_JOIN_TIMEOUT_SECONDS = 1
SHM_VERSION = 1

SHM_ERROR_HEADER_SIZE = "Shared-memory header has an unexpected size."
SHM_ERROR_HEADER_UNPACK = "Shared-memory header could not be unpacked."
SHM_ERROR_HEADER_MAGIC = "Unexpected shared-memory magic."
SHM_ERROR_UNSUPPORTED_VERSION = "Unsupported shared-memory version"
SHM_ERROR_UNSUPPORTED_PLATFORM = (
    "Shared-memory transport requires POSIX platform capabilities"
)
SHM_ERROR_CAPACITY = "Shared-memory capacity must be positive."
SHM_ERROR_CAPACITY_EXCEEDS_MAXIMUM = (
    "Shared-memory capacity exceeds configured maximum"
)
SHM_ERROR_CHANNEL_DIRECTORY_NOT_DIRECTORY = (
    "Shared-memory channel directory is not a directory"
)
SHM_ERROR_CHANNEL_DIRECTORY_NOT_OWNER = (
    "Shared-memory channel directory is not owned by the current user or root"
)
SHM_ERROR_CHANNEL_DIRECTORY_SYMLINK = (
    "Shared-memory channel directory is a symlink"
)
SHM_ERROR_CHANNEL_DIRECTORY_WORLD_WRITABLE = (
    "Shared-memory channel directory is writable by other users"
)
SHM_ERROR_CHANNEL_PATH_HAS_MULTIPLE_LINKS = (
    "Shared-memory channel path has multiple hard links"
)
SHM_ERROR_CHANNEL_PATH_NOT_REGULAR_FILE = (
    "Shared-memory channel path is not a regular file"
)
SHM_ERROR_CLOSE_LOCK_FILE = "Failed to close shared-memory lock file"
SHM_ERROR_CLOSE_MAP = "Failed to close shared-memory map"
SHM_ERROR_CLOSE_SIGNAL_FIFO = "Failed to close shared-memory signal FIFO"
SHM_ERROR_HANDLE_PAYLOAD = "Failed to handle shared-memory payload"
SHM_ERROR_PAYLOAD_LENGTH = "Shared-memory payload length exceeds configured capacity."
SHM_ERROR_READ_PAYLOAD = "Failed to read shared-memory payload"
SHM_ERROR_READER_LOCK_NOT_OPEN = "Shared-memory reader lock is not open."
SHM_ERROR_READER_NOT_OPEN = "Shared-memory reader is not open."
SHM_ERROR_READER_REMAP_FAILED = "Shared-memory reader remap failed."
SHM_ERROR_READER_STOPPING = "Shared-memory reader is still stopping."
SHM_ERROR_SIGNAL_FIFO_ACCESSIBLE_BY_OTHERS = (
    "Shared-memory signal FIFO is accessible by other users"
)
SHM_ERROR_SIGNAL_FIFO_NOT_OWNER = (
    "Shared-memory signal FIFO is not owned by the current user or root"
)
SHM_ERROR_SIGNAL_PATH_NOT_FIFO = "Shared-memory signal path is not a FIFO"
SHM_ERROR_SIGNAL_POLL_FAILED = "Shared-memory signal poll failed"
SHM_ERROR_SIGNAL_READ_FAILED = "Shared-memory signal read failed"
SHM_ERROR_WRITER_LOCK_NOT_OPEN = "Shared-memory writer lock is not open."
SHM_ERROR_WRITER_MAP_NOT_OPEN = "Shared-memory writer map is not open."
SHM_ERROR_WRITER_NOT_OPEN = "Shared-memory writer is not open."
SHM_FIFO_FULL_WARNING = "Shared-memory signal FIFO is full; coalescing wake-ups."
SHM_WARNING_READER_STILL_STOPPING = (
    "Shared-memory reader is still stopping; "
    "resources will close after its callback returns."
)
SHM_WARNING_RESET_HEADER = "Resetting shared-memory header at"
