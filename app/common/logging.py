import logging
import sys

from .config import settings


def setup_logging(component: str) -> logging.Logger:
    logging.basicConfig(
        stream=sys.stdout,
        level=settings.log_level.upper(),
        format=f"%(asctime)s %(levelname)s [{component}] %(name)s: %(message)s",
    )
    return logging.getLogger(component)
