# Pub-sub example

You can find this example in the directory

> `lib-dtps-http/python-examples/pub-sub`

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

The source code is in `pubsub1_publisher.py`.


### Entry point

This chunk is the entry point:

```python 
import argparse, asyncio
from dtps_http import DTPSServer, interpret_command_line_and_start
def simple_publisher() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Topic name")
    parsed, args = parser.parse_known_args()

    # We create a server and give it the on_startup callback to do work when
    # the server is ready
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    simple_publisher()
```

Things to notice:

1) we need `asyncio` to run the server, hence `asyncio.run` at the end of the main function.
2) we create a server using `DTPSServer.create` and pass it a callback `on_startup` to do work when the server is ready. 
3) we pass the server to `interpret_command_line_and_start` which runs the server based on the command line arguments, that describe what ports/sockets to listen to, and sets various other options.


### `on_startup`

This is the first part of the  `on_startup` callback that is called when the server is ready:

```python
from dtps_http import DTPSServer, async_error_catcher, MIME_JSON, ContentInfo, TopicNameV
from dtps_http.object_queue import ObjectQueue


@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    # We create a topic.
    # Topic name is given by the class TopicNameV which has parsing funcvtions
    OUT = TopicNameV.from_dash_sep("node/out")
    # We create the output queue.
    # You can give a lot of details
    queue_out: ObjectQueue = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON))
    ...
```

Things to notice:

1) The decorator `async_error_catcher` is used to log errors for async callbacks when they happen, independently of whether the task is being awaited. It's optional, but it's good practice.
2) we create a topic name using `TopicNameV.from_dash_sep` which parses a topic name from a string. Internally, topic names are possibly empty lists of strings.
3) we create an *output queue* using `s.create_oq` which takes a topic name and a :class:`~dtps_http.ContentInfo` object that describes the content of the queue. In this case, we use `ContentInfo.simple(MIME_JSON)` which means that the content of the queue are bytes to be interpreted as JSON strings. 


This is the second part:

```python
from dtps_http import DTPSServer, async_error_catcher
from dtps_http.object_queue import ObjectQueue
import asyncio


@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    ...

    queue_out: ObjectQueue = ...

    # We create a task that periodically publishes to the queue
    @async_error_catcher
    async def periodic_publish():
        i = 0
        while True:
            await asyncio.sleep(1)
            queue_out.publish_json({"counter": i})
            i += 1

    asyncio.create_task(periodic_publish())
```

This creates an asynchronous task that sleeps for 1 second and then publishes a JSON object to the output queue.


## Subscriber

The source code is in `pubsub1_subscriber.py`.


### Entrypoint

This is the main chunk, which parses the command line arguments and calls the function `read_continuous`:

```python
import argparse, asyncio
from dtps_http import URLString, parse_url_unescape


def subscribe_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()

    # Use parse_url_unescape to handle special unix socket urls with escaped slashes
    url = parse_url_unescape(URLString(parsed.url))
    f = read_continuous(url)
    asyncio.run(f)
    
```

Notice the use of `parse_url_unescape` to handle special unix socket urls with escaped slashes. 

### `read_continuous`

The `read_continuous` function takes a base URL of a topic,
creates an instance of a `DTPSClient`, and uses the `listen_continuous`
method to listen to the topic continuously. 

The idea of the `listen_continuous` method is to loop forever and be robust to
connections drops and other errors.

```python
from dtps_http import async_error_catcher, DTPSClient, URL


@async_error_catcher
async def read_continuous(urlbase0: URL) -> None:
    # Create an instance of the DTPSClient. Better to share it between
    # different requests as it caches some information.
    async with DTPSClient.create() as dtpsclient:
        async for rd in dtpsclient.listen_continuous(urlbase0,
                                                    add_silence=0.5,
                                                    raise_on_error=False,
                                                    ...):
            ...
            # now, processe the message m
```

The type of `rd` is one among `InsertNotification`, `WarningMsg`, `ErrorMsg`, `FinishedMsg`, `SilenceMsg`:

1) `InsertNotification` is the message that contains the data. It has two parts: `data_saved` and `raw_data`. `data_saved` contains metadata about the message, while `raw_data` contains the actual data, which is a byte array and a content type.
2) `WarningMsg` describes a warning. You can ignore it.
3) `ErrorMsg` are errors in the transmission of the message that are generated if `raise_on_error` is False. You should log or propagate them. In all cases, the underlying code will try to keep going (for example, by reconnecting to the server).
4) `FinishedMsg` means that the stream is over, and it is the last message you will get. If there was no `ErrorMsg` before, everything is ok.
5) `SilenceMsg` are generated periodically if you pass `add_silence` with the desired period. 

Therefore, to process the message, you should use a pattern match like the following
(or the equivalent `match` statement in Python 3.10):

```python
# you will get different kinds of messages

from dtps_http import InsertNotification, WarningMsg, ErrorMsg, FinishedMsg, SilenceMsg, DataSaved, RawData, ContentType, ListenURLEvents

rd: ListenURLEvents = ...

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
  ```
