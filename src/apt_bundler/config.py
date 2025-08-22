from __future__ import annotations
import os
from pathlib import Path

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUME_TOPIC   = os.getenv("CONSUME_TOPIC", "apt.requests")
PRODUCE_TOPIC   = os.getenv("PRODUCE_TOPIC", "apt.results")
GROUP_ID        = os.getenv("GROUP_ID", "apt-bundler-1")

# Output
OUT_DIR = Path(os.getenv("OUT_DIR", "/tmp"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

# APT behavior
DEFAULT_ARCH = os.getenv("ARCH", "amd64")
INCLUDE_RECOMMENDS = os.getenv("INCLUDE_RECOMMENDS", "1").lower() in ("1", "true", "yes")

# Simple constants for result statuses
STATUS_OK = "ok"
STATUS_ERR = "error"
