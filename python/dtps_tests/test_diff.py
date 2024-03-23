import asyncio
import copy
from typing import List
from unittest import IsolatedAsyncioTestCase

from jsonpatch import JsonPatch

from dtps import DTPSContext, PatchType
from dtps_http import (
    async_error_catcher,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_use_pair


class TestDiff(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_diff1(self):
        async with create_use_pair("call1") as (create, _use):
            topic: DTPSContext = await (create / "my_topic").queue_create()

            real: list[dict] = [{"a": 1}]
            patches: list[PatchType] = []

            async def listen_diff(otc: PatchType) -> None:
                patches.append(otc)

            await topic.subscribe_diff(listen_diff)

            for i in range(2, 5):
                letter = chr(i + 96)
                current = copy.deepcopy(real[-1])
                remove_one = next(iter(current))
                del current[remove_one]
                current[letter] = i
                real.append(current)

            for i in range(3):
                current = copy.deepcopy(real[-1])
                letter = chr(i + 97)
                current[letter] = {"b": i}
                real.append(current)

            for c in real:
                await topic.publish(RawData.cbor_from_native_object(c))

            await asyncio.sleep(3)

            logger.info(f"real: {real}")
            logger.info(f"patches: {patches}")
            reconstructed = reconstruct(patches, None)[1:]
            logger.info(f"reconstructed: {reconstructed}")
            self.assertEqual(real, reconstructed)


def reconstruct(patches: List[PatchType], initial: object) -> list[object]:
    all_states = [initial]
    for patch in patches:
        p = JsonPatch.from_string(patch, loads=lambda f: f)  # type: ignore
        current = p.apply(all_states[-1])
        all_states.append(current)

    return all_states
