import argparse
import asyncio
from typing import cast

from dtps_http import (
    async_error_catcher, DTPSClient, InsertNotification, parse_url_unescape,
    TopicNameV, TopicRef, URL, URLIndexer, URLString,
)


@async_error_catcher
async def read_continuous(urlbase0: URL) -> None:
    # we create a client, which caches some of the info
    async with DTPSClient.create() as dtpsclient:
        # we get the metadata
        md = await dtpsclient.get_metadata(urlbase0)
        # we check if it is a DTPS node
        if not md.answering:
            raise Exception("Not a DTPS node")
        # we get the topics inside
        index = await dtpsclient.ask_index(cast(URLIndexer, urlbase0))
        topics = index.topics
        tasks = []
        topic_name: TopicNameV
        info: TopicRef
        for topic_name, info in topics.items():
            # Exclude the topic itself
            if topic_name.is_root():  # exclude "":
                continue

            # info.reachability gives the (possibly relative url) where
            # to find the topic. We need to join it with the base url of the request
            # abs_url = join(urlbase0, info.reachability[0].url)
            abs_url = parse_url_unescape(info.reachability[0].url)
            print(f'- topic: {topic_name.__dict__!r}: {abs_url}')

            # creates a task that listens to the topic
            t = asyncio.create_task(listen(dtpsclient, abs_url, topic_name.as_dash_sep()))
            tasks.append(t)

        await asyncio.gather(*tasks)


async def listen(dtpsclient: DTPSClient, abs_url: URL, tn: str, ) -> None:
    async def callback(rd):
        if isinstance(rd, InsertNotification):
            print(f'{tn}: {rd.raw_data.content.decode("utf-8")}')
        else:
            print(f'{tn}: {rd!r}')

    ldi = await dtpsclient.listen_continuous(abs_url,
                                                 expect_node=None,
                                                 inline_data=True,
                                                 add_silence=None,
                                                 raise_on_error=False,
                                                 callback=callback,
                                                 switch_identity_ok=False)
       


def subscribe_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()

    # Use parse_url_unescape to handle special unix socket urls with escaped slashes
    url = parse_url_unescape(URLString(parsed.url))

    # run the async function
    f = read_continuous(url)
    asyncio.run(f)


if __name__ == '__main__':
    subscribe_main()
