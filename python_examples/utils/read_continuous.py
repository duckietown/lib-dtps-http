"""Read continuously."""

__all__ = ["read_continuous"]


from dtps_http import (
    URL,
    DTPSClient,
    ErrorMessage,
    FinishedMessage,
    InsertNotification,
    ListenURLEvents,
    NodeID,
    SilenceMessage,
    WarningMessage,
    async_error_catcher,
    logger,
)


async def callback(listen_url_events: ListenURLEvents) -> None:
    logger.info(listen_url_events)
    # You will get different kinds of messages
    if isinstance(listen_url_events, InsertNotification):
        # This is the data notification metadata about the
        # message, ...
        metadata = listen_url_events.data_saved
        # ... the raw data itself, ...
        raw_data = listen_url_events.raw_data
        # ... which is a pair of `content_type` (string) ...
        content_type = raw_data.content_type
        # ... and opaque bytes
        content = raw_data.content
    elif isinstance(listen_url_events, WarningMessage | ErrorMessage):
        # These are warnings and errors. You can decide to
        # ignore them or not. It is a good idea to log them in
        # any case.
        pass
    elif isinstance(listen_url_events, FinishedMessage):
        # This means that the stream is over. If there was no
        # `ErrorMessage` before, everything is okay.
        pass
    elif isinstance(listen_url_events, SilenceMessage):
        # This means that the stream has been silent for a
        # while. If you are using `add_silence`, you will get
        # this message.
        pass
    else:
        listen_url_events_class = type(listen_url_events)
        message = f"Unknown message type {listen_url_events_class}."
        raise TypeError(message)


@async_error_catcher
async def read_continuous(urlbase0: URL) -> None:
    """Read continuously."""
    # Create an instance of `DTPSClient`. Better to share it between
    # different requests as it caches some information.
    async with DTPSClient.create() as dtps_client:
        # Whether to expect a specific `NodeID` or not
        expect_node: NodeID | None = None
        # Whether to get inline data in the websocket or with a separate
        # HTTP request
        inline_data: bool = True
        # Whether to yield a silence message if the websocket does not
        # send anything for a while
        add_silence: float | None = 0.5
        # Whether to raise an exception if there are errors, or try to
        # keep going, with reconnections, etc
        raise_on_error: bool = False
        # Whether it is okay if after a reconnection, the node has
        # switched identity (e.g., when the node is restarted)
        switch_identity_ok: bool = False
        listen_data_interface = await dtps_client.listen_continuous(
            urlbase0,
            expect_node=expect_node,
            switch_identity_ok=switch_identity_ok,
            raise_on_error=raise_on_error,
            add_silence=add_silence,
            inline_data=inline_data,
            callback=callback,
            max_frequency=None,
        )
        await listen_data_interface.wait_for_done()
