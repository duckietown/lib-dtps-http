from .compat import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps import ContextConfig
from dtps_http import (
    async_error_catcher,
)
from dtps_http_tests.utils import test_timeout
from .utils import create_use_pair
from . import logger


class TestConfig(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_config1(self):
        async with create_use_pair("config1") as (c1, c2):
            for use in (c1, c2):
                logger.info(f"Testing {use}")
                config0 = ContextConfig.default()
                config1 = ContextConfig(patient=True)
                config2 = ContextConfig(patient=False)

                logger.info(f"config0={config0}")
                logger.info(f"config1={config1}")
                logger.info(f"config1={config1}")

                a: DTPSContext = use / "rpc"
                a1 = a.configure(config1)
                a2 = a.configure(config2)

                logger.info(f"a={a}")
                logger.info(f"a1={a1}")
                logger.info(f"a2={a2}")

                self.assertEqual(a.get_config(), config0)

                self.assertEqual(a1.get_config(), config1)
                self.assertEqual(a2.get_config(), config2)

                a1b = a1 / "b"
                a2b = a2 / "b"

                self.assertEqual(a1b.get_config(), config1)
                self.assertEqual(a2b.get_config(), config2)

                a1bc2 = a1b.configure(config2) / "x"
                a2bc1 = a2b.configure(config1) / "x"

                self.assertEqual(a1bc2.get_config(), config2)
                self.assertEqual(a2bc1.get_config(), config1)
