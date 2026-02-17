import os
from os import path

from pyflink.table import EnvironmentSettings, TableEnvironment

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

    # Kinesis source table via SQL DDL
    table_env.execute_sql(f"""
        CREATE TABLE kinesis_source (
            `message` STRING
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'input_stream',
            'aws.region' = 'us-east-1',
            'aws.credentials.provider' = 'BASIC',
            'aws.credentials.basic.access-key-id' = 'localstack_ignored',
            'aws.credentials.basic.secret-access-key' = 'localstack_ignored',
            'aws.endpoint' = '{KINESIS_ENDPOINT}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'raw'
        )
    """)

    # Kinesis sink table via SQL DDL
    table_env.execute_sql("""
        CREATE TABLE kinesis_sink (
            `message` STRING
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'output_stream',
            'aws.region' = 'us-east-1',
            'aws.credentials.provider' = 'BASIC',
            'aws.credentials.basic.access-key-id' = 'aws_access_key_id',
            'aws.credentials.basic.secret-access-key' = 'aws_secret_access_key',
            'aws.endpoint' = 'http://localhost:8000',
            'format' = 'raw'
        )
    """)

    # Pipeline via SQL DML
    table_env.execute_sql("""
        INSERT INTO kinesis_sink
        SELECT * FROM kinesis_source
    """).wait()


if __name__ == '__main__':
    run()
