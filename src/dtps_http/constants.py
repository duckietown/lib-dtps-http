# CONTENT_TYPE_TOPIC_DIRECTORY = "x-dtps-topics/json"
# CONTENT_TYPE_TOPIC_DESC = "x-dtps-topic/json"
from .types import TopicName

__all__ = [
    "CONTENT_TYPE_TOPIC_DESC",
    "CONTENT_TYPE_TOPIC_DIRECTORY",
    "HEADER_LINK_BENCHMARK",
    "HEADER_NODE_ID",
    "HEADER_NODE_PASSED_THROUGH",
    "HEADER_NO_CACHE",
    "HEADER_SEE_EVENTS",
    "TOPIC_LIST",
]
CONTENT_TYPE_TOPIC_DIRECTORY = CONTENT_TYPE_TOPIC_DESC = "application/json"
HEADER_SEE_EVENTS = "X-dtps-events"

HEADER_NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate"}
HEADER_NODE_ID = "X-DTPS-Node-ID"
HEADER_NODE_PASSED_THROUGH = "X-DTPS-Node-ID-Passed-Through"
HEADER_LINK_BENCHMARK = "X-DTPS-link-benchmark"
HEADER_DATA_UNIQUE_ID = "X-DTPS-data-unique-id"
HEADER_DATA_ORIGIN_NODE_ID = "X-DTPS-data-origin-node"
TOPIC_LIST = TopicName("__topic_list")
