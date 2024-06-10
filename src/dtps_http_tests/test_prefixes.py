from dtps_http import TopicNameV


def test_prefixes1() -> None:
    t = TopicNameV.from_relative_url("a/b/c/d/")
    prefixes = t.nontrivial_prefixes()
    assert prefixes == [
        TopicNameV.from_relative_url("a/"),
        TopicNameV.from_relative_url("a/b/"),
        TopicNameV.from_relative_url("a/b/c/"),
    ]


def test_prefixes2() -> None:
    t = TopicNameV.from_relative_url("a/")
    prefixes = t.nontrivial_prefixes()
    assert prefixes == []


def test_prefixes3() -> None:
    t = TopicNameV.root()
    prefixes = t.nontrivial_prefixes()
    assert prefixes == []
