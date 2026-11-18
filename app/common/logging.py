import logging
import sys

from .config import settings


def setup_logging(component: str) -> logging.Logger:
    from .tracing import TraceLogFilter
    logging.basicConfig(
        stream=sys.stdout,
        level=settings.log_level.upper(),
        format=f"%(asctime)s %(levelname)s [{component}]%(trace)s %(name)s: %(message)s",
    )
    for h in logging.getLogger().handlers:  # ' trace=<id>' on lines logged inside a traced operation
        h.addFilter(TraceLogFilter())
    return logging.getLogger(component)
