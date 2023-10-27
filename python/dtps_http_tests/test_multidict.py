from multidict import CIMultiDict

from dtps_http import multidict_update


def test_multidict1() -> None:
    d1 = CIMultiDict()

    d1.add("a", "1")
    d1.add("a", "2")
    # print(f'{d1=!r}')

    d2 = CIMultiDict()
    d2.add("a", "3")

    d2.update(d1)
    print(f"{d2=!r}")
    # print(d2)
    assert list(d2) == ["a", "a"]

    d3 = CIMultiDict()
    d3.add("a", "3")
    multidict_update(d3, d1)
    # print(f'{d3=!r}')

    assert list(d3) == ["a", "a", "a"]
