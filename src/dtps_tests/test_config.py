"""Configuration test."""

from unittest import IsolatedAsyncioTestCase

from dtps import ContextConfig
from dtps_http import async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


class TestConfig(IsolatedAsyncioTestCase):
    """Configuration test."""

    @staticmethod
    @test_timeout(20)
    @async_error_catcher
    async def test_config() -> None:
        """Run configuration test."""
        async with create_use_pair("config1") as (c1, c2):
            for use in (c1, c2):
                logger.info(f"Testing {use}")
                config0 = ContextConfig.default()
                config1 = ContextConfig(patient=True)
                config2 = ContextConfig(patient=False)
                logger.info("config0=%s", config0)
                logger.info("config1=%s", config1)
                logger.info("config2=%s", config2)
                a = use / "rpc"
                a1 = a.configure(config1)
                a2 = a.configure(config2)
                logger.info("a=%s", a)
                logger.info("a1=%s", a1)
                logger.info("a2=%s", a2)
                if a.get_config() != config0:
                    raise AssertionError
                if a1.get_config() != config1:
                    raise AssertionError
                if a2.get_config() != config2:
                    raise AssertionError
                a1b = a1 / "b"
                a2b = a2 / "b"
                if a1b.get_config() != config1:
                    raise AssertionError
                if a2b.get_config() != config2:
                    raise AssertionError
                a1bc2 = a1b.configure(config2) / "x"
                a2bc1 = a2b.configure(config1) / "x"
                if a1bc2.get_config() != config2:
                    raise AssertionError
                if a2bc1.get_config() != config1:
                    raise AssertionError
