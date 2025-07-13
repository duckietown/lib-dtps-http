"""Prefix tests."""

from dtps_http import TopicNameV


def test_prefixes1() -> None:
    """Run first prefixes test."""
    topic_name = TopicNameV.from_relative_url("a/b/c/d/")
    nontrivial_prefixes = topic_name.nontrivial_prefixes()
    nontrivial_prefix_1 = TopicNameV.from_relative_url("a/")
    nontrivial_prefix_2 = TopicNameV.from_relative_url("a/b/")
    nontrivial_prefix_3 = TopicNameV.from_relative_url("a/b/c/")
    if nontrivial_prefixes != [
        nontrivial_prefix_1,
        nontrivial_prefix_2,
        nontrivial_prefix_3,
    ]:
        raise AssertionError


def test_prefixes2() -> None:
    """Run second prefixes test."""
    topic_name = TopicNameV.from_relative_url("a/")
    nontrivial_prefixes = topic_name.nontrivial_prefixes()
    if nontrivial_prefixes != []:
        raise AssertionError


def test_prefixes3() -> None:
    """Run third prefixes test."""
    topic_name = TopicNameV.root()
    nontrivial_prefixes = topic_name.nontrivial_prefixes()
    if nontrivial_prefixes != []:
        raise AssertionError
