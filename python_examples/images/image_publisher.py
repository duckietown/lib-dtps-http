"""Image publisher."""

import asyncio

import cv2
import numpy as np

from dtps_http import (
    MIME_JPEG,
    ContentInfo,
    DTPSServer,
    ObjectQueue,
    RawData,
    TopicNameV,
    async_error_catcher,
    interpret_command_line_and_start,
    logger,
)
from dtps_http.structures import Bounds


# Create a task that periodically publishes to the queue
@async_error_catcher
async def video_reader(queue_out: ObjectQueue) -> None:
    """Run video reader."""
    loop = asyncio.get_running_loop()
    video_capture = cv2.VideoCapture(0)
    logger.info("Opening video capture...")
    while True:
        # Read a frame from the video
        # We use run_in_executor to run the blocking call in a
        # thread (very important)
        returned, frame = await loop.run_in_executor(
            None,
            video_capture.read,
        )
        # Check if the video has ended
        if not returned:
            break
        _, jpeg_encoded_frame = cv2.imencode(".jpg", frame)
        # Convert the JPEG encoded frame to bytes
        jpeg_array = np.array(jpeg_encoded_frame)
        jpeg_data = jpeg_array.tobytes()
        raw_data = RawData(content=jpeg_data, content_type=MIME_JPEG)
        await queue_out.publish(raw_data)


@async_error_catcher
async def on_startup(server: DTPSServer) -> None:
    """Run on startup."""
    # Create a topic.
    # The topic name is given by the `TopicNameV` class which has
    # parsing functions
    topic_name = TopicNameV.from_dash_sep("node/out")
    # Create the output queue
    content_info = ContentInfo.simple(MIME_JPEG)
    bounds = Bounds.max_length(2)
    queue_out = await server.create_object_queue(
        topic_name,
        content_info,
        topic_properties=None,
        bounds=bounds,
    )
    coroutine = video_reader(queue_out)
    asyncio.create_task(coroutine)


def image_publisher() -> None:
    """Run image publisher."""
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    coroutine = interpret_command_line_and_start(dtps_server)
    asyncio.run(coroutine)


if __name__ == "__main__":
    image_publisher()
