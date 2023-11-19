__all__ = [
    "DTPSClientException",
    "EventListeningNotAvailable",
    "NoSuchTopic",
]


class DTPSException(Exception):
    pass


class DTPSClientException(DTPSException):
    pass


class EventListeningNotAvailable(DTPSClientException):
    pass


class NoSuchTopic(DTPSClientException):
    pass
