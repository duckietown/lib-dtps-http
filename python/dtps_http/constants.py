from .types import TopicNameV

__all__ = [
    "CONTENT_TYPE_DTPS_INDEX",
    "CONTENT_TYPE_DTPS_INDEX_CBOR",
    "CONTENT_TYPE_TOPIC_DESC",
    "CONTENT_TYPE_TOPIC_DIRECTORY",
    "EVENTS_SUFFIX",
    "HEADER_CONTENT_LOCATION",
    "HEADER_DATA_ORIGIN_NODE_ID",
    "HEADER_DATA_UNIQUE_ID",
    "HEADER_LINK_BENCHMARK",
    "HEADER_NODE_ID",
    "HEADER_NODE_PASSED_THROUGH",
    "HEADER_NO_AVAIL",
    "HEADER_NO_CACHE",
    "REL_EVENTS_DATA",
    "REL_EVENTS_NODATA",
    "REL_META",
    "REL_URL_META",
    "TOPIC_AVAILABILITY",
    "TOPIC_CLOCK",
    "TOPIC_LIST",
    "TOPIC_LIST",
    "TOPIC_LOGS",
    "TOPIC_STATE_NOTIFICATION",
    "TOPIC_STATE_SUMMARY",
]

CONTENT_TYPE_TOPIC_DIRECTORY = CONTENT_TYPE_TOPIC_DESC = "application/json"
# HEADER_SEE_EVENTS = "X-dtps-events"
# HEADER_SEE_EVENTS_INLINE_DATA = "X-dtps-events-inline-data"

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
TOPIC_STATE_NOTIFICATION = TopicNameV.from_dash_sep("dtps/states-notification")

CONTENT_TYPE_DTPS_INDEX = "application/vnd.dt.dtps-index"
CONTENT_TYPE_DTPS_INDEX_CBOR = "application/vnd.dt.dtps-index+cbor"
CONTENT_TYPE_TOPIC_HISTORY_CBOR = "application/vnd.dt.dtps-history+cbor"

HEADER_NO_AVAIL = "X-dtps-debug-Content-Location-Not-Available"
HEADER_CONTENT_LOCATION = "Content-Location"

REL_EVENTS_NODATA = "dtps-events"
REL_EVENTS_DATA = "dtps-events-inline-data"
REL_META = "dtps-meta"
REL_HISTORY = "dtps-history"

EVENTS_SUFFIX = ":events"
REL_URL_META = ":meta"
REL_URL_HISTORY = ":history"
