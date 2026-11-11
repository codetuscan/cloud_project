import hashlib
import re

_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def fmt(hexdigest: str) -> str:
    return f"sha256:{hexdigest}"


def valid(value: str | None) -> bool:
    return bool(value and _RE.match(value))


def sha256_bytes(data: bytes) -> str:
    return fmt(hashlib.sha256(data).hexdigest())
