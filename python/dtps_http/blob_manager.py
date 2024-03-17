import time
from dataclasses import dataclass
from typing import Dict, Set, Tuple

from .structures import (
    Digest,
    get_digest,
)

__all__ = [
    "BlobManager",
]

from .utils_every_once_in_a_while import EveryOnceInAWhile


@dataclass
class SavedBlob:
    content: bytes
    who_needs_it: Set[Tuple[str, int]]
    deadline: float


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
        todrop = []

        for digest, sb in list(self.blobs.items()):
            no_one_needs_it = len(sb.who_needs_it) == 0
            deadline_passed = now > sb.deadline
            if no_one_needs_it and deadline_passed:
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

    def release_blob(self, digest: Digest, who_needs_it: Tuple[str, int]):
        if digest not in self.blobs:
            return
        sb = self.blobs[digest]
        sb.who_needs_it.remove(who_needs_it)
        if len(sb.who_needs_it) == 0:
            deadline_passed = time.time() > sb.deadline
            if deadline_passed:
                self.blobs.pop(digest, None)
                self.blobs_forgotten[digest] = time.time()

    def save_blob(self, content: bytes, who_needs_it: Tuple[str, int]) -> Digest:
        self.cleanup_blobs_if_its_time()
        digest = get_digest(content)
        if digest not in self.blobs:
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it={who_needs_it},
                deadline=time.time(),
            )
        else:
            sb = self.blobs[digest]
            sb.who_needs_it.add(who_needs_it)
        return digest

    def save_blob_deadline(self, content: bytes, deadline: float) -> Digest:
        self.cleanup_blobs_if_its_time()
        digest = get_digest(content)
        if digest not in self.blobs:
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it=set(),
                deadline=deadline,
            )
        else:
            sb = self.blobs[digest]
            sb.deadline = max(deadline, sb.deadline)
        return digest

    def extend_deadline(self, digest: Digest, seconds: float) -> None:
        if digest not in self.blobs:
            return
        sb = self.blobs[digest]
        new_deadline = time.time() + seconds
        sb.deadline = max(sb.deadline, new_deadline)
