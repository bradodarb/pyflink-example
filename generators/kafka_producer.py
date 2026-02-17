import json
import random
import time
import argparse

from kafka import KafkaProducer


def load_records(path: str) -> list:
    with open(path) as f:
        return json.load(f)


def randomize_value(value: float) -> float:
    drift = value * random.uniform(-0.1, 0.1)
    return round(value + drift, 2)


def produce(records: list, topic: str, bootstrap_servers: str, delay_ms: int, loops: int):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    for i in range(loops):
        for record in records:
            payload = {
                **record,
                "timestamp": str(int(time.time() * 1000)),
                "value": randomize_value(record["value"]),
            }
            producer.send(topic, key=str(record["id"]), value=payload)
            print(f"[loop {i+1}/{loops}] put {json.dumps(payload)}")
            time.sleep(delay_ms / 1000.0)

    producer.flush()
    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce sensor records to Kafka")
    parser.add_argument("--file", default="generators/sensors.json", help="Path to JSON records file")
    parser.add_argument("--topic", default="input_topic", help="Kafka topic name")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--delay", type=int, default=100, help="Delay between records in ms")
    parser.add_argument("--loops", type=int, default=1, help="Number of times to iterate through the records")
    args = parser.parse_args()

    records = load_records(args.file)
    produce(records, args.topic, args.brokers, args.delay, args.loops)
