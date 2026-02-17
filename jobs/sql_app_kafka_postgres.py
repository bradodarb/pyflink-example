import os
from os import path

from pyflink.table import EnvironmentSettings, TableEnvironment

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

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # Kafka source table with JSON format via SQL DDL
    table_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            `id` STRING,
            `kind` STRING,
            `value` STRING,
            `timestamp` STRING
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'topic' = 'input_topic',
            'properties.group.id' = 'kafka_postgres_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
    """)

    # JDBC/Postgres sink table with upsert via primary key
    table_env.execute_sql(f"""
        CREATE TABLE postgres_sink (
            `id` STRING,
            `kind` STRING,
            `value` STRING,
            `timestamp` STRING,
            PRIMARY KEY (`id`, `timestamp`) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = 'sensor_readings',
            'driver' = 'org.postgresql.Driver',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}'
        )
    """)

    # Pipeline via SQL DML with null filtering
    table_env.execute_sql("""
        INSERT INTO postgres_sink
        SELECT * FROM kafka_source
        WHERE `id` IS NOT NULL
    """).wait()


if __name__ == '__main__':
    run()
