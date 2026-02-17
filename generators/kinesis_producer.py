import json
import os
import random
import time
import argparse

import boto3


def load_records(path: str) -> list:
    with open(path) as f:
        return json.load(f)


def randomize_value(value: float) -> float:
    drift = value * random.uniform(-0.1, 0.1)
    return round(value + drift, 2)


def produce(records: list, stream_name: str, endpoint_url: str, delay_ms: int, loops: int):
    client = boto3.client(
        "kinesis",
        endpoint_url=endpoint_url,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )

    for i in range(loops):
        for record in records:
            payload = {
                **record,
                "timestamp": str(int(time.time() * 1000)),
                "value": randomize_value(record["value"]),
            }
            data = json.dumps(payload)
            client.put_record(
                StreamName=stream_name,
                Data=data.encode("utf-8"),
                PartitionKey=str(record["id"]),
            )
            print(f"[loop {i+1}/{loops}] put {data}")
            time.sleep(delay_ms / 1000.0)

    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce sensor records to Kinesis")
    parser.add_argument("--file", default="generators/sensors.json", help="Path to JSON records file")
    parser.add_argument("--stream", default="input_stream", help="Kinesis stream name")
    parser.add_argument("--endpoint", default="http://localhost:4566", help="Kinesis endpoint URL")
    parser.add_argument("--delay", type=int, default=100, help="Delay between records in ms")
    parser.add_argument("--loops", type=int, default=1, help="Number of times to iterate through the records")
    args = parser.parse_args()

    records = load_records(args.file)
    produce(records, args.stream, args.endpoint, args.delay, args.loops)
