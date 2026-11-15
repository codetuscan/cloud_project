import pytest

from app.common import chunking

MIB = 1_048_576


def test_exact_multiple():
    assert chunking.chunk_count(2 * MIB, MIB) == 2
    assert chunking.layout(2 * MIB, MIB) == [(0, 0, MIB), (1, MIB, MIB)]


def test_one_byte_over_makes_a_tiny_last_chunk():
    assert chunking.chunk_count(MIB + 1, MIB) == 2
    assert chunking.chunk_span(1, MIB + 1, MIB) == (MIB, 1)


def test_smaller_than_one_chunk():
    assert chunking.layout(10, MIB) == [(0, 0, 10)]


def test_exactly_one_chunk():
    assert chunking.layout(MIB, MIB) == [(0, 0, MIB)]


def test_zero_bytes_has_no_chunks():
    assert chunking.chunk_count(0, MIB) == 0
    assert chunking.layout(0, MIB) == []


def test_5_5_mib_is_six_chunks_last_half_mib():
    size = 5 * MIB + MIB // 2
    lay = chunking.layout(size, MIB)
    assert len(lay) == 6
    assert lay[-1] == (5, 5 * MIB, MIB // 2)


def test_layout_covers_object_exactly_once_in_order():
    for size in (1, 7, 1000, 1023, 1024, 1025, 99999):
        lay = chunking.layout(size, 1024)
        assert sum(length for _, _, length in lay) == size
        assert [i for i, _, _ in lay] == list(range(len(lay)))
        pos = 0
        for _, off, length in lay:
            assert off == pos
            pos += length


def test_decimal_megabyte_chunk_size_is_supported():
    assert chunking.chunk_count(2_500_000, 1_000_000) == 3


def test_invalid_arguments():
    with pytest.raises(ValueError):
        chunking.chunk_count(10, 0)
    with pytest.raises(IndexError):
        chunking.chunk_span(2, MIB + 1, MIB)
