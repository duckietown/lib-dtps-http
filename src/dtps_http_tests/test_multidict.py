"""Multidict tests."""

from multidict import CIMultiDict

from dtps_http import multidict_update


def test_multidict() -> None:
    """Run multidict test."""
    d1: CIMultiDict[str] = CIMultiDict()
    d1.add("a", "1")
    d1.add("a", "2")
    d2: CIMultiDict[str] = CIMultiDict()
    d2.add("a", "3")
    d2.update(d1)
    if tuple(d2) != ("a", "a"):
        raise AssertionError
    d3: CIMultiDict[str] = CIMultiDict()
    d3.add("a", "3")
    multidict_update(d3, d1)
    if tuple(d3) != ("a", "a", "a"):
        raise AssertionError
