import asyncio

import numpy as np

from dtps_http import (
    async_error_catcher, ContentInfo, DTPSServer, interpret_command_line_and_start, logger,
    MIME_JPEG, RawData, TopicNameV,
)


@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    # We create a topic.
    # Topic name is given by the class TopicNameV which has parsing funcvtions
    OUT = TopicNameV.from_dash_sep("node/out")
    # We create the output queue.
    # You can give a lot of details
    queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JPEG))

    # We create a task that periodically publishes to the queue
    @async_error_catcher
    async def video_reader():
        import cv2
        loop = asyncio.get_running_loop()
        video_capture = cv2.VideoCapture(0)
        logger.info("Opening video capture")
        while True:
            # Read a frame from the video

            # Very important: we use run_in_executor to run the blocking call in a thread
            ret, frame = await loop.run_in_executor(None, video_capture.read)

            # Check if the video has ended
            if not ret:
                break

            _, jpeg_bytes = cv2.imencode('.jpg', frame)

            # Convert the JPEG bytes to a byte array
            jpeg_byte_array = np.array(jpeg_bytes).tobytes()

            rd = RawData(content=jpeg_byte_array, content_type=MIME_JPEG)
            await queue_out.publish(rd)

    asyncio.create_task(video_reader())


def image_publisher() -> None:
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    asyncio.run(interpret_command_line_and_start(dtps_server))


if __name__ == '__main__':
    image_publisher()
