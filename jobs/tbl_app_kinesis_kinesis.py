import os
from os import path

from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor,
                           Schema, DataTypes, FormatDescriptor)

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)
KINESIS_ENDPOINT = os.getenv('KINESIS_ENDPOINT', 'http://localhost:4566')


def run():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")

    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        table_env.get_config().set("pipeline.jars", f"file:///{jar_location}")
        table_env.get_config().set("pipeline.classpaths", f"file:///{jar_location}")

    # Kinesis source table definition
    table_env.create_temporary_table(
        "kinesis_source",
        TableDescriptor.for_connector("kinesis")
        .schema(Schema.new_builder()
                .column("message", DataTypes.STRING())
                .build())
        .option("stream", "input_stream")
        .option("aws.region", "us-east-1")
        .option("aws.credentials.provider", "BASIC")
        .option("aws.credentials.basic.access-key-id", "localstack_ignored")
        .option("aws.credentials.basic.secret-access-key", "localstack_ignored")
        .option("aws.endpoint", KINESIS_ENDPOINT)
        .option("scan.startup.mode", "latest-offset")
        .format(FormatDescriptor.for_format("raw").build())
        .build()
    )

    # Kinesis sink table definition
    table_env.create_temporary_table(
        "kinesis_sink",
        TableDescriptor.for_connector("kinesis")
        .schema(Schema.new_builder()
                .column("message", DataTypes.STRING())
                .build())
        .option("stream", "output_stream")
        .option("aws.region", "us-east-1")
        .option("aws.credentials.provider", "BASIC")
        .option("aws.credentials.basic.access-key-id", "aws_access_key_id")
        .option("aws.credentials.basic.secret-access-key", "aws_secret_access_key")
        .option("aws.endpoint", "http://localhost:8000")
        .format(FormatDescriptor.for_format("raw").build())
        .build()
    )

    # Read from source and insert into sink using the Table API
    source_table = table_env.from_path("kinesis_source")
    source_table.execute_insert("kinesis_sink").wait()


if __name__ == '__main__':
    run()
