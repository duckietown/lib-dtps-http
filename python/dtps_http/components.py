__all__ = [  # "components_from_topicpart",
    # "join_topic_names",
    # "topicpart_from_components",
]

#
# def components_from_topicpart(s: TopicName | tuple[str, ...]) -> tuple[str, ...]:
#     if isinstance(s, tuple):
#         return s
#     if not s:
#         return ()
#     return tuple(s.split("."))
#
#
# def topicpart_from_components(components: tuple[str, ...]) -> TopicName:
#     return cast(TopicName, ".".join(components))
#
#
# def join_topic_names(prefix: TopicName | tuple[str, ...], name: TopicName | tuple[str, ...]) -> TopicName:
#     parts = components_from_topicpart(prefix) + components_from_topicpart(name)
#     return topicpart_from_components(parts)
