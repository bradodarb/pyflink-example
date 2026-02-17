import os
from os import path

from pyflink.table import EnvironmentSettings, TableEnvironment

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

    # Kafka source table via SQL DDL
    table_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `message` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{brokers}',
            'topic' = 'input_topic',
            'properties.group.id' = 'stream_example',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)

    # Kafka sink table via SQL DDL
    table_env.execute_sql(f"""
        CREATE TABLE kafka_sink (
            `message` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{brokers}',
            'topic' = 'output_topic',
            'sink.delivery-guarantee' = 'at-least-once',
            'format' = 'raw'
        )
    """)

    # Pipeline via SQL DML
    table_env.execute_sql("""
        INSERT INTO kafka_sink
        SELECT * FROM kafka_source
    """).wait()


if __name__ == '__main__':
    run()
