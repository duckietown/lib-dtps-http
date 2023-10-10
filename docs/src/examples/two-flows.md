# Multiple flows example

You can find this example in the directory

> `lib-dtps-http/python-examples/two-flows`

There are two scripts in this example: a publisher and a subscriber.

## Running the example


In two different terminals, run:

```bash
make publisher
```

```bash
make subscriber
```

## Publisher 

The source code is in `twoflows_publisher.py`.

This is very similar to the publisher in the [`pub-sub` example](pub-sub.md), but it creates  multiple topics that update at different frequency. 

## Subscriber

The source code is in `twoflows_subscriber.py`.

This subscriber is more complex than the one in the [`pub-sub` example](pub-sub.md) because it performs discovery of the topics and subscribes to multiple topics.

The url passed to the program, `urlbase0` is now meant to be the url of the entire node, and the program will discover the topics and subscribe to them.

```python
from dtps_http import DTPSClient, URL,TopicNameV, TopicRef, join

async def go(urlbase0: URL) -> None:
    async with DTPSClient.create() as dtpsclient:
        # we get the metadata
        md = await dtpsclient.get_metadata(urlbase0)
        # we check if it is a DTPS node
        if not md.answering:
            raise Exception("Not a DTPS node")
        # we get the topics inside
        topics = await dtpsclient.ask_topics(urlbase0)
        topic_name: TopicNameV
        info: TopicRef
        for topic_name, info in topics.items():
            # Exclude the root node
            if topic_name.is_root():  # exclude "":
                continue
            # info.reachability gives the (possibly relative url) where
            # to find the topic. We need to join it with the base url of the request
            abs_url = join(urlbase0, info.reachability[0].url)
            
            # now listen to the topic

```

The two new methods of `DTPSClient` used are:

1) `get_metadata` to get the metadata of the node. This is a `Metadata` object that contains metadata about the URL. If the node is a DTPS node, `answering` will contain the node ID.
2) `ask_topics` is used to get the topics inside the node. It returns a dictionary of `TopicNameV` (the name of the topic) and `TopicRef` (the metadata of the topic).

The class `TopicRef` contains the field `reachability` which is a list of `TopicReachability` objects, which contain the `url` field.
All urls in DTPS are meant to be interpreted *relative to the url of the request*; therefore, to obtain the absolute url, we need to use `join` to join the base url of the request with the url of the topic. 
