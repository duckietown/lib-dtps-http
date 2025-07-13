"""Exceptions."""

__all__ = [
    "ConditionSatistiedError",
    "DTPSClientError",
    "DTPSError",
    "EventListeningNotAvailableError",
    "NoSuchTopicError",
    "ShutdownAskedError",
    "StopContinuousLoopError",
    "TopicOriginUnavailableError",
]


class ConditionSatistiedError(Exception):
    """Condition satistied error."""


class DTPSError(Exception):
    """DTPS error."""


class DTPSClientError(DTPSError):
    """DTPS client error."""


class EventListeningNotAvailableError(DTPSError):
    """Event listening not available error."""


class NoSuchTopicError(DTPSError):
    """No such topic error."""


class ShutdownAskedError(Exception):
    """Shutdown asked error."""


class StopContinuousLoopError(Exception):
    """Stop continuous loop error."""


class TopicOriginUnavailableError(DTPSError):
    """Topic origin unavailable error."""
