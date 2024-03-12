import os

from .types import ContentType, TopicNameV

__all__ = [
    "CONTENT_TYPE_DTPS_DATAREADY_CBOR",
    "CONTENT_TYPE_DTPS_INDEX",
    "CONTENT_TYPE_DTPS_INDEX_CBOR",
    "CONTENT_TYPE_PATCH_CBOR",
    "CONTENT_TYPE_PATCH_JSON",
    "CONTENT_TYPE_PATCH_YAML",
    "CONTENT_TYPE_TOPIC_HISTORY_CBOR",
    "ENV_MASK_ORIGIN",
    "EVENTS_SUFFIX",
    "HEADER_CONTENT_LOCATION",
    "HEADER_DATA_ORIGIN_NODE_ID",
    "HEADER_DATA_UNIQUE_ID",
    "HEADER_LINK_BENCHMARK",
    "HEADER_NODE_ID",
    "HEADER_NODE_PASSED_THROUGH",
    "HEADER_NO_AVAIL",
    "HEADER_NO_CACHE",
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
    "REL_URL_HISTORY",
    "REL_URL_META",
    "TOPIC_AVAILABILITY",
    "TOPIC_CLOCK",
    "TOPIC_CONNECTIONS",
    "TOPIC_LIST",
    "TOPIC_LOGS",
    "TOPIC_PROXIED",
    "TOPIC_STATE_NOTIFICATION",
    "TOPIC_STATE_SUMMARY",
    "DEFAULT_MAX_HISTORY"
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

DEFAULT_MAX_HISTORY: int = 1
