import os
from os import path

from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor,
                           Schema, DataTypes, FormatDescriptor)

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka0:29092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_container')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'changeme')


def run():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")

    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        table_env.get_config().set("pipeline.jars", f"file:///{jar_location}")
        table_env.get_config().set("pipeline.classpaths", f"file:///{jar_location}")

    # Kafka source table with JSON format matching sensor_readings schema
    table_env.create_temporary_table(
        "kafka_source",
        TableDescriptor.for_connector("kafka")
        .schema(Schema.new_builder()
                .column("id", DataTypes.STRING())
                .column("kind", DataTypes.STRING())
                .column("value", DataTypes.STRING())
                .column("timestamp", DataTypes.STRING())
                .build())
        .option("properties.bootstrap.servers", KAFKA_BROKERS)
        .option("topic", "input_topic")
        .option("properties.group.id", "kafka_postgres_group")
        .option("scan.startup.mode", "latest-offset")
        .format(FormatDescriptor.for_format("json")
                .option("fail-on-missing-field", "false")
                .build())
        .build()
    )

    # JDBC/Postgres sink table with upsert support via primary key
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    table_env.create_temporary_table(
        "postgres_sink",
        TableDescriptor.for_connector("jdbc")
        .schema(Schema.new_builder()
                .column("id", DataTypes.STRING())
                .column("kind", DataTypes.STRING())
                .column("value", DataTypes.STRING())
                .column("timestamp", DataTypes.STRING())
                .primary_key("id", "timestamp")
                .build())
        .option("url", jdbc_url)
        .option("table-name", "sensor_readings")
        .option("driver", "org.postgresql.Driver")
        .option("username", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .build()
    )

    # Read from Kafka, filter nulls, and upsert into Postgres
    source_table = table_env.from_path("kafka_source")
    filtered = source_table.filter(source_table.id.is_not_null)
    filtered.execute_insert("postgres_sink").wait()


if __name__ == '__main__':
    run()
