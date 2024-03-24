import argparse
import asyncio
from pprint import pprint
from typing import Optional

from dtps_http import (
    async_error_catcher, ContentType, DataSaved, DTPSClient, ErrorMsg, FinishedMsg,
    InsertNotification, ListenURLEvents, NodeID, parse_url_unescape, RawData, SilenceMsg, URL, URLString,
    WarningMsg,
)


@async_error_catcher
async def read_continuous(urlbase0: URL) -> None:
    # Create an instance of the DTPSClient. Better to share it between
    # different requests as it caches some information.
    async with DTPSClient.create() as dtpsclient:
        # Whether to expect a specific NodeID or not
        expect_node: Optional[NodeID] = None
        # Whether to get inline data in the websocket
        # or with a separate HTTP request
        inline_data: bool = True
        # Whether to yield a silence message if the websocket
        # does not send anything for a while
        add_silence: Optional[float] = 0.5
        # Whether to raise an exception if there are errors,
        # or trying to keep going, with reconnections, etc.
        raise_on_error: bool = False
        # Whether it's ok if after a reconnection the node has switched identity
        # (e.g. when the node is restarted)
        switch_identity_ok: bool = False

        async def callback(rd: ListenURLEvents) -> None:
            pprint(rd)

            # you will get different kinds of messages
            if isinstance(rd, InsertNotification):
                # This is the data notification

                # metadata about the message
                metadata: DataSaved = rd.data_saved
                # the data itself
                raw_data: RawData = rd.raw_data
                # which is a pair of content_type (string)
                content_type: ContentType = raw_data.content_type
                # ... and opaque bytes
                content: bytes = raw_data.content

            elif isinstance(rd, (WarningMsg, ErrorMsg)):
                # These are warnings and errors.
                # You can decide to ignore them or not.
                # A good idea to log them in any case.
                pass
            elif isinstance(rd, FinishedMsg):
                # This means that the stream is over.
                # If there was no ErrorMsg before, everything is ok.
                pass
            elif isinstance(rd, SilenceMsg):
                # This means that the stream has been silent for a while.
                # If you are using add_silence, you will get this message.
                pass
            else:
                raise Exception(f"Unknown message type {type(rd)}")

        ldi = await dtpsclient.listen_continuous(urlbase0,
                                           expect_node=expect_node,
                                           inline_data=inline_data,
                                           add_silence=add_silence,
                                           raise_on_error=raise_on_error,
                                           callback=callback,
                                           switch_identity_ok=switch_identity_ok,
                                           max_frequency=None)
        await ldi.wait_for_done()

def subscribe_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()

    # Use parse_url_unescape to handle special unix socket urls with escaped slashes
    url = parse_url_unescape(URLString(parsed.url))
    f = read_continuous(url)
    asyncio.run(f)


if __name__ == '__main__':
    subscribe_main()
