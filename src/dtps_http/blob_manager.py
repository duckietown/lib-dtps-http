"""Blob manager."""

__all__ = ["BlobManager"]

import base64
import time
import uuid
from dataclasses import dataclass

import dtps_http
from dtps_http.structures import Digest
from dtps_http.types_ import URLString
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile


@dataclass
class SavedBlob:
    """Saved blob."""

    content: bytes
    who_needs_it: set[tuple[str, int]]
    outstanding_tokens: dict[str, float]

    @classmethod
    def make(cls, content: bytes) -> "SavedBlob":
        return cls(content, set(), {})

    def clean_old(self, now: float) -> None:
        outstanding_tokens_items = self.outstanding_tokens.items()
        for token, deadline in list(outstanding_tokens_items):
            if deadline < now:
                self.outstanding_tokens.pop(token, None)

    def someone_needs_it(self) -> bool:
        return len(self.who_needs_it) > 0 or len(self.outstanding_tokens) > 0


class BlobManager:
    """Blob manager."""

    blobs: dict[Digest, SavedBlob]
    blobs_forgotten: dict[Digest, float]
    cleanup_interval: float
    cleanup_time: EveryOnceInAWhile
    forget_forgetting_interval: float

    def __init__(
        self,
        *,
        cleanup_interval: float,
        forget_forgetting_interval: float,
    ) -> None:
        """Initialize blob manager."""
        self.blobs = {}
        self.blobs_forgotten = {}
        self.cleanup_interval = cleanup_interval
        self.forget_forgetting_interval = forget_forgetting_interval
        self.cleanup_time = EveryOnceInAWhile(cleanup_interval)

    def clean_up_blobs_if_it_is_time(self) -> None:
        """Clean up blobs if it is time."""
        if self.cleanup_time.now():
            self.clean_up_blobs()

    def clean_up_blobs(self) -> None:
        """Clean up blobs."""
        current_time = time.time()
        digest_to_drop_list: list[Digest] = []
        blob_items = self.blobs.items()
        for digest, saved_blob in list(blob_items):
            saved_blob.clean_old(current_time)
            if not saved_blob.someone_needs_it():
                digest_to_drop_list.append(digest)
        for digest_to_drop in digest_to_drop_list:
            self.blobs.pop(digest_to_drop, None)
            self.blobs_forgotten[digest_to_drop] = current_time
        forgotten_blob_items = self.blobs_forgotten.items()
        for digest, ts in list(forgotten_blob_items):
            if current_time - ts > self.forget_forgetting_interval:
                self.blobs_forgotten.pop(digest, None)

    def has_blob(self, digest: Digest) -> bool:
        """Return `True` if blob manager has blob, `False` otherwise."""
        return digest in self.blobs

    def get_blob(self, digest: Digest) -> bytes:
        """Return blob."""
        if digest not in self.blobs:
            if digest in self.blobs_forgotten:
                message = f"Blob {digest} was forgotten."
                raise KeyError(message)
            message = f"Blob {digest} not found and never known."
            raise KeyError(message)
        saved_blob = self.blobs[digest]
        return saved_blob.content

    def get_blob_once(self, digest: Digest, token: str) -> bytes:
        """Return blob once."""
        if digest not in self.blobs:
            if digest in self.blobs_forgotten:
                message = f"Blob {digest} was forgotten."
                raise KeyError(message)
            message = f"Blob {digest} not found and never known."
            raise KeyError(message)
        saved_blob = self.blobs[digest]
        if token not in saved_blob.outstanding_tokens:
            pass
        else:
            saved_blob.outstanding_tokens.pop(token, None)
        if not saved_blob.someone_needs_it():
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = time.time()
        return saved_blob.content

    def release_blob(
        self,
        digest: Digest,
        who_needs_it: tuple[str, int],
    ) -> None:
        """Release blob."""
        if digest not in self.blobs:
            return
        saved_blob = self.blobs[digest]
        saved_blob.who_needs_it.remove(who_needs_it)
        current_time = time.time()
        saved_blob.clean_old(current_time)
        if not saved_blob.someone_needs_it():
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = time.time()

    def save_blob_for_queue(
        self,
        content: bytes,
        who_needs_it: tuple[str, int],
    ) -> Digest:
        """Save blob for queue."""
        self.clean_up_blobs_if_it_is_time()
        digest = dtps_http.get_digest(content)
        saved_blob = self._save_blob(digest, content)
        saved_blob.who_needs_it.add(who_needs_it)
        return digest

    def get_use_once_link_store(
        self,
        digest: Digest,
        content: bytes,
        content_type: str,
        max_availability: float,
    ) -> URLString:
        """Return use-once link store."""
        saved_blob = self._save_blob(digest, content)
        uuid4 = uuid.uuid4()
        token = str(uuid4)
        current_time = time.time()
        saved_blob.outstanding_tokens[token] = current_time + max_availability
        return encode_url2(digest, content_type, token)

    def _save_blob(self, digest: Digest, content: bytes) -> SavedBlob:
        if digest not in self.blobs:
            who_needs_it: set[tuple[str, int]] = set()
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it=who_needs_it,
                outstanding_tokens={},
            )
        return self.blobs[digest]


def encode_url2(digest: Digest, content_type: str, token: str) -> URLString:
    if not content_type:
        message = "Cannot encode url for empty content type."
        raise ValueError(message)
    encoded_content_type = content_type.encode()
    b64_encoded_content_type = base64.urlsafe_b64encode(encoded_content_type)
    b64_content_type = b64_encoded_content_type.decode("ascii")
    return URLString(f"./:blobs/{digest}/{b64_content_type}/{token}")
