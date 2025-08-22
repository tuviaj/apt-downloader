from __future__ import annotations
import json
from confluent_kafka import Consumer, Producer
from config import KAFKA_BOOTSTRAP, GROUP_ID

def make_consumer(topic: str) -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    c.subscribe([topic])
    return c

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def send_json(producer: Producer, topic: str, payload: dict) -> None:
    producer.produce(topic, json.dumps(payload).encode("utf-8"))
    producer.flush()
