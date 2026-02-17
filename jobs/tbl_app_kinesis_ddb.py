import os
from os import path

from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor,
                           Schema, DataTypes, FormatDescriptor)

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)
KINESIS_ENDPOINT = os.getenv('KINESIS_ENDPOINT', 'http://localstack:4566')
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT', 'http://dynamodb-local:8000')


def run():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")

    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        table_env.get_config().set("pipeline.jars", f"file:///{jar_location}")
        table_env.get_config().set("pipeline.classpaths", f"file:///{jar_location}")

    # Kinesis source table with JSON format
    table_env.create_temporary_table(
        "kinesis_source",
        TableDescriptor.for_connector("kinesis")
        .schema(Schema.new_builder()
                .column("id", DataTypes.STRING())
                .column("message", DataTypes.STRING())
                .build())
        .option("stream", "input_stream")
        .option("aws.region", "us-east-1")
        .option("aws.credentials.provider", "BASIC")
        .option("aws.credentials.basic.access-key-id", "test")
        .option("aws.credentials.basic.secret-access-key", "test")
        .option("aws.endpoint", KINESIS_ENDPOINT)
        .option("scan.startup.mode", "latest-offset")
        .format(FormatDescriptor.for_format("json")
                .option("fail-on-missing-field", "false")
                .build())
        .build()
    )

    # DynamoDB sink table (columns map to DynamoDB attributes)
    table_env.create_temporary_table(
        "dynamodb_sink",
        TableDescriptor.for_connector("dynamodb")
        .schema(Schema.new_builder()
                .column("id", DataTypes.STRING())
                .column("message", DataTypes.STRING())
                .primary_key("id")
                .build())
        .option("table-name", "PyFlinkTestTable")
        .option("aws.region", "us-east-1")
        .option("aws.credentials.provider", "BASIC")
        .option("aws.credentials.basic.access-key-id", "test")
        .option("aws.credentials.basic.secret-access-key", "test")
        .option("aws.endpoint", DYNAMODB_ENDPOINT)
        .build()
    )

    # Read from Kinesis, filter empty records, and insert into DynamoDB
    source_table = table_env.from_path("kinesis_source")
    filtered = source_table.filter(source_table.id.is_not_null)
    filtered.execute_insert("dynamodb_sink").wait()


if __name__ == '__main__':
    run()
