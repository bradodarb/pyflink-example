import os
from os import path

from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor,
                           Schema, DataTypes, FormatDescriptor)

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)


def run():
    brokers = "localhost:9092"
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")

    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        table_env.get_config().set("pipeline.jars", f"file:///{jar_location}")
        table_env.get_config().set("pipeline.classpaths", f"file:///{jar_location}")

    # Kafka source table definition
    table_env.create_temporary_table(
        "kafka_source",
        TableDescriptor.for_connector("kafka")
        .schema(Schema.new_builder()
                .column("message", DataTypes.STRING())
                .build())
        .option("properties.bootstrap.servers", brokers)
        .option("topic", "input_topic")
        .option("properties.group.id", "stream_example")
        .option("scan.startup.mode", "earliest-offset")
        .format(FormatDescriptor.for_format("raw").build())
        .build()
    )

    # Kafka sink table definition
    table_env.create_temporary_table(
        "kafka_sink",
        TableDescriptor.for_connector("kafka")
        .schema(Schema.new_builder()
                .column("message", DataTypes.STRING())
                .build())
        .option("properties.bootstrap.servers", brokers)
        .option("topic", "output_topic")
        .option("sink.delivery-guarantee", "at-least-once")
        .format(FormatDescriptor.for_format("raw").build())
        .build()
    )

    # Read from source and insert into sink using the Table API
    source_table = table_env.from_path("kafka_source")
    source_table.execute_insert("kafka_sink").wait()


if __name__ == '__main__':
    run()
