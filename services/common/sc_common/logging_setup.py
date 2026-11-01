"""JSON structured logging to stdout.

Fluent Bit tails container stdout and ships it to Loki, so one JSON object per
line is all that is needed for centralized, queryable logs
(e.g. `{app="booking-service"} | json | level="ERROR"`).
"""
import json
import logging
import sys
import time

from . import config

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime", "color_message"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": config.SERVICE_NAME,
            "pod": config.POD_NAME,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO") -> logging.Logger:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("uvicorn.access", "botocore", "urllib3", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(config.SERVICE_NAME)
