"""Difference test."""

import asyncio
import copy
from typing import Any
from unittest import IsolatedAsyncioTestCase

from jsonpatch import JsonPatch

from dtps import PatchType
from dtps_http import RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


class TestDiff(IsolatedAsyncioTestCase):
    """Difference test."""

    @staticmethod
    def _get_listen_diff(patches: list[PatchType]) -> Any:
        async def listen_diff(otc: PatchType) -> None:
            patches.append(otc)

        return listen_diff

    @test_timeout(20)
    @async_error_catcher
    async def test_diff(self) -> None:
        """Run difference test."""
        async with create_use_pair("call1") as (create, _):
            queue = create / "my_topic"
            await queue.queue_create()
            real = [
                {
                    "a": 1,
                },
            ]
            patches: list[PatchType] = []
            listen_diff = self._get_listen_diff(patches)
            await queue.subscribe_diff(listen_diff)
            for i in range(2, 5):
                letter = chr(i + 96)
                current = copy.deepcopy(real[-1])
                current_iterator = iter(current)
                remove_one = next(current_iterator)
                del current[remove_one]
                current[letter] = i
                real.append(current)
            for i in range(3):
                current = copy.deepcopy(real[-1])
                letter = chr(i + 97)
                current[letter] = i
                real.append(current)
            for object_ in real:
                raw_data = RawData.cbor_from_native_object(object_)
                await queue.publish(raw_data)
            await asyncio.sleep(3)
            logger.info("real: %s", real)
            logger.info("patches: %s", patches)
            reconstructed = reconstruct(patches, None)[1:]
            logger.info("reconstructed: %s", reconstructed)
            if real != reconstructed:
                raise AssertionError


def reconstruct(patches: list[PatchType], initial: object) -> list[object]:
    """Reconstruct."""
    all_states = [initial]
    for patch in patches:
        json_patch = JsonPatch.from_string(patch, loads=lambda f: f)
        current = json_patch.apply(all_states[-1])
        all_states.append(current)
    return all_states
