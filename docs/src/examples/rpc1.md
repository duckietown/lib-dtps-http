# RPC example 1

You can find this example in the directory

> `lib-dtps-http/python-examples/rpc1`

## Running the example

Run:

```bash
make
```

## RPC support

The source code is in `rpc1.py`.

### The transform function

The new thing is the presence of a transform function that is called on the data before it is put in the queue.
This can serve as an entry point for RPC calls.

In this example we expect that the input is a json/yaml/cbor even integer.

```python
from dtps_http import RawData
from dtps_http.object_queue import ObjectTransformContext, ObjectTransformResult, TransformError


async def transform(x: ObjectTransformContext) -> ObjectTransformResult:
    # use this to get the raw data
    raw_data_input = x.raw_data

    # interpet json, yaml, cbor, etc.
    number = raw_data_input.get_as_native_object()
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

```

The input is a `ObjectTransformContext` object. The `raw_data` field contains the raw data.

The output must be a `ObjectTransformResult`, which is the union of `RawData` or  `TransformError`.

`TransformError` is an object that contains the HTTP status code and a message.

The 40x status codes are used to indicate that the client made a mistake, e.g. the input was not an integer.

### Setting the transform function

The transform function is set in the `create_oq()` function call:

```python

queue_in = await s.create_oq(..., transform=transform)
```
