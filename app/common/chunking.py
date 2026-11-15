"""Pure chunk arithmetic (no I/O): how an object of `size` bytes is cut into fixed-size chunks."""


def chunk_count(size: int, chunk_size: int) -> int:
    if size < 0 or chunk_size <= 0:
        raise ValueError("size must be >= 0 and chunk_size > 0")
    return -(-size // chunk_size)  # ceiling division; 0 bytes -> 0 chunks


def chunk_span(index: int, size: int, chunk_size: int) -> tuple[int, int]:
    """(offset, length) of chunk `index`; every chunk is chunk_size long except possibly the last."""
    if not 0 <= index < chunk_count(size, chunk_size):
        raise IndexError(f"chunk {index} out of range for size {size}")
    offset = index * chunk_size
    return offset, min(chunk_size, size - offset)


def layout(size: int, chunk_size: int) -> list[tuple[int, int, int]]:
    """[(index, offset, length), ...] covering the object exactly once, in order."""
    return [(i, *chunk_span(i, size, chunk_size)) for i in range(chunk_count(size, chunk_size))]
