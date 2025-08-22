from __future__ import annotations
import json
from confluent_kafka import Message
from config import CONSUME_TOPIC, PRODUCE_TOPIC, STATUS_ERR
from kafka_io import make_consumer, make_producer, send_json
from apt_bundle import process_job

def _msg_to_job(msg: Message) -> dict | None:
    try:
        return json.loads(msg.value().decode("utf-8"))
    except Exception as e:
        return {"_parse_error": str(e)}

def main() -> None:
    consumer = make_consumer(CONSUME_TOPIC)
    producer = make_producer()
    print(f"[worker] listening on '{CONSUME_TOPIC}', producing to '{PRODUCE_TOPIC}'")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[worker] consumer error: {msg.error()}")
                continue

            job = _msg_to_job(msg)
            if job is None or "_parse_error" in job:
                send_json(producer, PRODUCE_TOPIC, {
                    "status": STATUS_ERR,
                    "error": f"invalid JSON: {job.get('_parse_error') if job else 'unknown'}"
                })
                consumer.commit(msg)
                continue

            result = process_job(job)
            send_json(producer, PRODUCE_TOPIC, result)
            consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
