"""Two-flows subscriber."""

import argparse
import asyncio
from typing import Any, cast

from dtps_http import (
    URL,
    DTPSClient,
    InsertNotification,
    ListenURLEvents,
    URLIndexer,
    URLString,
    async_error_catcher,
    logger,
    parse_url_unescape,
)


@async_error_catcher
async def read_continuous(urlbase0: URL) -> None:
    """Read continuously."""
    # Create a client, which caches some of the info
    async with DTPSClient.create() as dtpsclient:
        # Get the metadata
        metadata = await dtpsclient.get_metadata(urlbase0)
        # Check if it is a DTPS node
        if not metadata.answering:
            message = "Not a DTPS node."
            raise Exception(message)
        # Get the topics inside
        url_indexer = cast(URLIndexer, urlbase0)
        index = await dtpsclient.ask_index(url_indexer)
        topics = index.topics
        tasks = []
        for topic_name, info in topics.items():
            # Exclude the topic itself
            if topic_name.is_root():
                continue
            # `info.reachability` gives the (possibly relative) URL
            # where to find the topic. We need to join it with the base
            # URL of the request
            abs_url = parse_url_unescape(info.reachability[0].url)
            logger.info("- topic: %r: %s", topic_name.__dict__, abs_url)
            # creates a task that listens to the topic
            dash_separated_topic_name = topic_name.as_dash_sep()
            coroutine = listen(dtpsclient, abs_url, dash_separated_topic_name)
            task = asyncio.create_task(coroutine)
            tasks.append(task)
        await asyncio.gather(*tasks)


def get_callback(dash_separated_topic_name: str) -> Any:
    """Return `callback`."""

    async def callback(listen_url_events: ListenURLEvents) -> None:
        if isinstance(listen_url_events, InsertNotification):
            decoded_content = listen_url_events.raw_data.content.decode(
                "utf-8",
            )
            logger.info("%s: %s", dash_separated_topic_name, decoded_content)
        else:
            logger.info("%s: %r", dash_separated_topic_name, listen_url_events)

    return callback


async def listen(
    dtpsclient: DTPSClient,
    abs_url: URL,
    dash_separated_topic_name: str,
) -> None:
    """Listen."""
    callback = get_callback(dash_separated_topic_name)
    await dtpsclient.listen_continuous(
        abs_url,
        expect_node=None,
        inline_data=True,
        add_silence=None,
        raise_on_error=False,
        callback=callback,
        switch_identity_ok=False,
        max_frequency=None,
    )


def subscribe_main() -> None:
    """Run subscribe."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()
    # Use `parse_url_unescape` to handle special unix socket urls with
    # escaped slashes
    url_string = URLString(parsed.url)
    url = parse_url_unescape(url_string)
    # Run the asyncronous function
    coroutine = read_continuous(url)
    asyncio.run(coroutine)


if __name__ == "__main__":
    subscribe_main()
