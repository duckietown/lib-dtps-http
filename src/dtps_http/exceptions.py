__all__ = [
    "DTPSClientException",
    "DTPSException",
    "EventListeningNotAvailable",
    "NoSuchTopic",
    "TopicOriginUnavailable",
]


class DTPSException(Exception):
    pass


class DTPSClientException(DTPSException):
    pass


class EventListeningNotAvailable(DTPSClientException):
    pass


class NoSuchTopic(DTPSClientException):
    pass


class TopicOriginUnavailable(DTPSClientException):
    pass
