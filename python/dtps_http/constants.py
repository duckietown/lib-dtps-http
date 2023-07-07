# CONTENT_TYPE_TOPIC_DIRECTORY = "x-dtps-topics/json"
# CONTENT_TYPE_TOPIC_DESC = "x-dtps-topic/json"
from .types import TopicName

__all__ = [
    "CONTENT_TYPE_TOPIC_DESC",
    "CONTENT_TYPE_TOPIC_DIRECTORY",
    "HEADER_CONTENT_LOCATION",
    "HEADER_DATA_ORIGIN_NODE_ID",
    "HEADER_DATA_UNIQUE_ID",
    "HEADER_LINK_BENCHMARK",
    "HEADER_NODE_ID",
    "HEADER_NODE_PASSED_THROUGH",
    "HEADER_NO_AVAIL",
    "HEADER_NO_CACHE",
    "HEADER_SEE_EVENTS",
    "HEADER_SEE_EVENTS_INLINE_DATA",
    "TOPIC_LIST",
]
CONTENT_TYPE_TOPIC_DIRECTORY = CONTENT_TYPE_TOPIC_DESC = "application/json"
HEADER_SEE_EVENTS = "X-dtps-events"
HEADER_SEE_EVENTS_INLINE_DATA = "X-dtps-events-inline-data"

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
TOPIC_LIST = TopicName("__topic_list")

HEADER_NO_AVAIL = "X-dtps-debug-Content-Location-Not-Available"
HEADER_CONTENT_LOCATION = "Content-Location"
