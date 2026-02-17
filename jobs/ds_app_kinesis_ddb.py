import os
from os import path
from typing import Dict

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (StreamExecutionEnvironment, RuntimeExecutionMode)
from pyflink.datastream.connectors.kinesis import (FlinkKinesisConsumer)
from pyflink.java_gateway import get_gateway

from framework.connectors.dynamodb import DynamoDbSink

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)
KINESIS_ENDPOINT = os.getenv('KINESIS_ENDPOINT', 'http://localstack:4566')
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT', 'http://dynamodb-local:8000')


def get_source(stream_name: str, config: Dict = None) -> FlinkKinesisConsumer:
    props = config or {}
    consumer_config = {
        'aws.region': 'us-east-1',
        'aws.credentials.provider': 'BASIC',
        'aws.credentials.provider.basic.accesskeyid': 'test',
        'aws.credentials.provider.basic.secretkey': 'test',
        'flink.stream.initpos': 'LATEST',
        'aws.endpoint': KINESIS_ENDPOINT,
        **props
    }
    return FlinkKinesisConsumer(stream_name, SimpleStringSchema(), consumer_config)


def get_sink(table_name: str, config: Dict = None) -> DynamoDbSink:
    props = config or {}
    return (DynamoDbSink(**{
        'table.name': table_name,
        'aws.region': 'us-east-1',
        'aws.credentials.provider': 'BASIC',
        'aws.credentials.provider.basic.accesskeyid': 'test',
        'aws.credentials.provider.basic.secretkey': 'test',
        'aws.endpoint': DYNAMODB_ENDPOINT,
        **props
    }))


def run():
    get_gateway()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # To enable local running/debugging, we manually add the project's shadow jar that has all the connectors built in
    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        env.add_jars(f"file:///{jar_location}")
        env.add_classpaths(f"file:///{jar_location}")

    # Build a Datastream from the Kinesis source
    stream = env.add_source(get_source('input_stream'))

    # Filter out empty records
    stream = stream.filter(lambda record: record is not None and len(record.strip()) > 0)

    # Sink to DynamoDB
    sink = get_sink('PyFlinkTestTable')
    stream.sink_to(sink)

    env.execute("kinesis-2-dynamoDB")


if __name__ == '__main__':
    run()
