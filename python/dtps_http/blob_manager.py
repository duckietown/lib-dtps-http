import base64
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Set, Tuple

from .structures import (
    Digest,
    get_digest,
)
from .types import URLString
from .utils_every_once_in_a_while import EveryOnceInAWhile

__all__ = [
    "BlobManager",
]


@dataclass
class SavedBlob:
    content: bytes
    who_needs_it: Set[Tuple[str, int]]

    # token with deadline
    outstanding_tokens: Dict[str, float]

    @classmethod
    def make(cls, content: bytes) -> "SavedBlob":
        return cls(content, set(), {})

    def clean_old(self, now: float) -> None:
        for token, deadline in list(self.outstanding_tokens.items()):
            if deadline < now:
                self.outstanding_tokens.pop(token, None)

    def someone_needs_it(self) -> bool:
        return len(self.who_needs_it) > 0 or len(self.outstanding_tokens) > 0


class BlobManager:
    blobs: Dict[Digest, SavedBlob]
    blobs_forgotten: Dict[Digest, float]

    forget_forgetting_interval: float
    cleanup_interval: float
    cleanup_time: EveryOnceInAWhile

    def __init__(self, *, forget_forgetting_interval: float, cleanup_interval: float):
        self.blobs = {}
        self.blobs_forgotten = {}
        self.forget_forgetting_interval = forget_forgetting_interval
        self.cleanup_interval = cleanup_interval
        self.cleanup_time = EveryOnceInAWhile(cleanup_interval)

    def cleanup_blobs_if_its_time(self) -> None:
        if self.cleanup_time.now():
            self.cleanup_blobs()

    def cleanup_blobs(self) -> None:
        now = time.time()
        todrop: list[Digest] = []

        for digest, sb in list(self.blobs.items()):
            sb.clean_old(now)
            if not sb.someone_needs_it():
                todrop.append(digest)

        for digest in todrop:
            # print(f"Dropping blob {digest} because deadline passed")
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = now

        # forget the forgotten blobs

        for digest, ts in list(self.blobs_forgotten.items()):
            if now - ts > self.forget_forgetting_interval:
                self.blobs_forgotten.pop(digest, None)

    def get_blob(self, digest: Digest) -> bytes:
        if digest not in self.blobs:
            if digest in self.blobs_forgotten:
                raise KeyError(f"Blob {digest} was forgotten")
            raise KeyError(f"Blob {digest} not found and never known")
        sb = self.blobs[digest]
        return sb.content

    def get_blob_once(self, digest: Digest, token: str) -> bytes:
        if digest not in self.blobs:
            if digest in self.blobs_forgotten:
                raise KeyError(f"Blob {digest} was forgotten")
            raise KeyError(f"Blob {digest} not found and never known")
        sb = self.blobs[digest]
        if token not in sb.outstanding_tokens:
            pass
            # raise KeyError(f"Token {token} not found for blob {digest}")
        else:
            sb.outstanding_tokens.pop(token, None)

        if not sb.someone_needs_it():
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = time.time()
        return sb.content

    def release_blob(self, digest: Digest, who_needs_it: Tuple[str, int]):
        if digest not in self.blobs:
            return
        sb = self.blobs[digest]

        sb.who_needs_it.remove(who_needs_it)
        now = time.time()
        sb.clean_old(now)
        if not sb.someone_needs_it():
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = time.time()

    def save_blob_for_queue(self, content: bytes, who_needs_it: Tuple[str, int]) -> Digest:
        self.cleanup_blobs_if_its_time()
        digest = get_digest(content)
        sb = self._save_blob(digest, content)
        sb.who_needs_it.add(who_needs_it)

        return digest

    def get_use_once_link_store(
        self, digest: Digest, content: bytes, content_type: str, max_availability_s: float
    ) -> URLString:
        sb = self._save_blob(digest, content)

        token = str(uuid.uuid4())
        now = time.time()
        sb.outstanding_tokens[token] = now + max_availability_s

        return encode_url2(digest, content_type, token)

    def _save_blob(self, digest: Digest, content: bytes) -> SavedBlob:
        if digest not in self.blobs:
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it=set(),
                outstanding_tokens={},
            )
        return self.blobs[digest]


def encode_url2(digest: Digest, content_type: str, token: str) -> URLString:
    if not content_type:
        raise ValueError(f"Cannot encode url for empty content type")
    b64 = base64.urlsafe_b64encode(content_type.encode()).decode("ascii")

    url = URLString(f"./:blobs/{digest}/{b64}/{token}")
    return url
