__all__ = [
    "DTPSClientException",
    "EventListeningNotAvailable",
    "NoSuchTopic",
]


class DTPSClientException(Exception):
    pass


class EventListeningNotAvailable(DTPSClientException):
    pass


class NoSuchTopic(DTPSClientException):
    pass
