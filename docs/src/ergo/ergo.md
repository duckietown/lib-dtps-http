# Ergonomic DTPS API

This part describes the "ergonomic" DTPS API.

## Environment variables and contexts

One goal of the ergonomic API is to make the nodes configurable from the outside, without having to change the code, if one wants
to change the DTPS "coordinates" on which they should operate.

We call "contexts" the "entrypoints" into a DTPS hierarchy.

Contexts are created from environment variables of the form `DTPS_BASE_<context name>`.

By convention, the context name is `self` for the `main` context.

The basic configuration simply points the context to a URL.

For example:

    DTPS_BASE_SELF = "http://localhost:2120/" # use an existing server

Or we can use a unix socket:

    DTPS_BASE_SELF = "http+unix://[socket]/" # use an existing unix socket

### Creating servers

In addition, we can use the special prefix `create:` to create a new DTPS server.

For example:

    DTPS_BASE_SELF = "create:http://localhost:2120/" # create a new server

In this way we can create a node that can either become a server or use another server as a client.
Either way, it is transparent to the user.

### Creating servers with multiple listening points

A DTPS server can listen on multiple interfaces/sockets at the same time.

To enable this functionality, we can use a suffix `_<number>`:

    DTPS_BASE_SELF_0 = "create:http://localhost:2120/" #
    DTPS_BASE_SELF_1 = "create:http+unix://[socket]/"

This will create a server that listens on both `localhost:2120` and on the unix socket `[socket]`.

## Initializing contexts

To initialize a context from the code, use `context`:

```python
from dtps import context


async def main():
    c = await context("self")
    ...

    await c.aclose()

```

Alternatively there is a context manager that will cleanup after itself:

```python
from dtps import context_cleanup


async def main():
    async with context_cleanup("self") as c:
        ...
```

One can easily use multiple contexts. For example, if the environment variables are:

    DTPS_BASE_NODE1 = "http://localhost:2001/" 
    DTPS_BASE_NODE2 = "http://localhost:2002/"

Then we can use them as follows:

```python
from dtps import context_cleanup


async def main():
    async with context_cleanup("node1") as node1:
        async with context_cleanup("node2") as node2:
            ...


```

## DTPSContext interface

The API exposed is given by the `DTPSContext` interface described in the file [API](../api).

### Navigating the hierarchy of resources

Navigation is done using the `navigate` method:

```python
from dtps import DTPSContext

context: DTPSContext = ...
child = context.navigate('a', 'b', 'c')
```

Slashes are normalized, so the following is equivalent:

```python
child = context.navigate('a/b/c')
```

Or, as a convenient shortcut, we can use the `/` operator:

```python

child = context / 'a' / 'b' / 'c'

child = context / 'a/b' / 'c'  # same

```

### Creating and deleting queues and publishing data

To create a queue, use the `queue_create()` method.
Use the `remove()` method to delete a queue.

### Publishing data: one-off `publish`

To publish data, use the `publish` method with a `RawData` object:

```python
from dtps import RawData, context


async def example1_process_data() -> None:
    # DTPS_BASE_SELF = "create:http://:8000/"

    me = await context("self")

    node_output = await (me / "dtps" / "node" / "out").queue_create()

    rd = RawData(b"Hello World", "text/plain")
    await node_output.publish(rd)

```

### Publishing data: continuous publishing

If you are trying to publish a stream of data, you should use the `publisher_context` method
which returns a publisher object:


```python

from dtps import DTPSContext, RawData

async def example_continuous(context: DTPSContext):
    
    async with context.publisher_context() as publisher:
        for _ in range(10):
            data = RawData(b"Hello World", "text/plain")
            await publisher.publish(data)
```

In `dtps-http`, this is mapped to a websocket-based publishing mechanism, as opposed to a POST-based mechanism.

### One-off data get

The `data_get` method is used to retrieve the data from a queue.

```python
from dtps import DTPSContext, RawData
async def get_data(context: DTPSContext):
    data: RawData = await context.data_get()
```
 

### Subscribing to updates

The `subscribe` method returns a `Subscription` object that can be used to receive messages.

With this example we combine the two methods to create a simple echo server
that receives messages from a queue and publishes them back to another queue:

```python
import asyncio
from dtps import RawData, context


async def example2_process_data() -> None:
    # Environment example:
    #   DTPS_BASE_SOURCE = "http://:8000/dtps/node/out" 
    #   DTPS_BASE_TARGET= "http://:8001/dtps/node/in" 

    source = await context("source")
    target = await context("target")

    async def on_input(data: RawData) -> None:
        await target.publish(data)

    subscription = await source.subscribe(on_input)

    await asyncio.sleep(1000)

    await subscription.unsubscribe()  # close the subscription

```

### Exposing / "mounting"

The `expose` method is used to proxy a resource.

For example, in the following example we assume that there is an external switchboard.
We will create a node and expose it to the switchboard:

```python
from dtps import context


async def example2_register() -> None:
    # Environment example:
    #   DTPS_BASE_SELF_0 = "create:http://:212/"
    #   DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
    #   DTPS_BASE_SWITCHBOARD = "http://:8000/"
    me = await context("self")
    switchboard = await context("switchboard")

    await switchboard.navigate("dtps/node/nodename").expose(me)
```

The result is that the node will be availabel through the switchboard starting at the URL `http://:8000/dtps/node/nodename`.

### Connecting topics

The `connect` method is used to connect a node output to a node input.

Note: This only works on the Rust server.

The following example shows how to connect `dtps/node/node1` to `dtps/node/node2` on a switchboard. Here, it is assumed
that `node1` and `node2` are already exposed to the switchboard.

```python
from dtps import context


async def example4_connect2() -> None:
    # Environment example:
    #   DTPS_BASE_SWITCHBOARD = "http://:8000/dtps/node/nodename/"
    #   DTPS_BASE_SWITCHBOARD_ADD_1 = "http+unix://:8000/dtps/node/nodename/"

    switchboard = await context("switchboard_add")

    node1_out = switchboard / "dtps/node/node1"
    node2_in = switchboard / "dtps/node/node2"

    await node1_out.connect_to(node2_in)
```


### Patch mechanism

DTPS supports a patch mechanism that allows to modify resources using the JSON-patch
mechanism.

Note: while the standard used to define the patch mechanism is JSON-patch (RFC 6902), this
can be used also on CBOR and YAML resources.

```python 
from dtps import DTPSContext, RawData
async def example_patch(context: DTPSContext):
    # first, publish this as CBOR    
    ob1 = {"a": 1}
    rd = RawData.cbor_from_native_object(ob1)
    await context.publish(rd)

    # now create a JSON patch to update the value of "a"
    patch1 = [{"op": "replace", "path": "/a", "value": 2}]
    await context.patch(patch1)
    
    # now read the value back
    rd2 = await context.data_get()
    
    rd2_expected = RawData.cbor_from_native_object({"a": 2})
    assert rd2 == rd2_expected

```



### RPC mechanism using `call()`


The `call()` method is used to call a remote procedure.
It takes a `RawData` object as input and returns a `RawData` object as output.


```python 
from dtps import DTPSContext, RawData
async def example_call(context: DTPSContext): 
    rd = RawData(b"Hello World", "text/plain")
    result: RawData = await context.call(rd)
```

From the server side, you can create a handler for the call by passing a parameter `transform` to the `queue_create()` method.

The idea is that the `transform` parameter is a function that takes a `RawData` object as input and returns a `RawData` object as output, and the transformed data is what is put in the queue.
If there is an error, the callback should return a `TransformError` with an error code and a message.

The following is a complete example:

```python

from dtps import RawData, context, TransformError
from typing import Union


async def example_listen() -> None:
    # DTPS_BASE_SELF = "create:http://:8000/"

    me = await context("self")

    async def transform(rd: RawData, /) -> Union[RawData, TransformError]:
    
        # interpret json, yaml, cbor, etc. as a phython object
        number = rd.get_as_native_object()
        if not isinstance(number, int):
            # return error
            return TransformError(400, f"Expected an integer for this parameter, got {type(number)}")
    
        if number % 2 == 0:
            # if the number is even, we return a string
            result = RawData.cbor_from_native_object(
                {'ok': True, 'msg': f"Got an even number {number}"}
            )
            return result
        else:
            # return error
            return TransformError(400, f"Expected an even integer, got {number}")
      
    await (me / "rpc").queue_create(transform=transform)

    
```
