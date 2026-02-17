import os
from os import path

from pyflink.table import EnvironmentSettings, TableEnvironment

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

    # Kinesis source table with JSON format via SQL DDL
    table_env.execute_sql(f"""
        CREATE TABLE kinesis_source (
            `id` STRING,
            `message` STRING
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'input_stream',
            'aws.region' = 'us-east-1',
            'aws.credentials.provider' = 'BASIC',
            'aws.credentials.basic.access-key-id' = 'test',
            'aws.credentials.basic.secret-access-key' = 'test',
            'aws.endpoint' = '{KINESIS_ENDPOINT}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
    """)

    # DynamoDB sink table (columns map to DynamoDB attributes)
    table_env.execute_sql(f"""
        CREATE TABLE dynamodb_sink (
            `id` STRING,
            `message` STRING,
            PRIMARY KEY (`id`) NOT ENFORCED
        ) WITH (
            'connector' = 'dynamodb',
            'table-name' = 'PyFlinkTestTable',
            'aws.region' = 'us-east-1',
            'aws.credentials.provider' = 'BASIC',
            'aws.credentials.basic.access-key-id' = 'test',
            'aws.credentials.basic.secret-access-key' = 'test',
            'aws.endpoint' = '{DYNAMODB_ENDPOINT}'
        )
    """)

    # Pipeline via SQL DML with null filtering
    table_env.execute_sql("""
        INSERT INTO dynamodb_sink
        SELECT * FROM kinesis_source
        WHERE `id` IS NOT NULL
    """).wait()


if __name__ == '__main__':
    run()
