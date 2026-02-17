import os
from os import path

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.java_gateway import get_gateway

LOCAL_DEBUG = os.getenv('LOCAL_DEBUG', False)
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka0:29092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_container')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'changeme')

SQL = (
    "INSERT INTO sensor_readings (id, kind, value, timestamp) VALUES (?, ?, ?, ?)"
    " ON CONFLICT (id, timestamp) DO UPDATE SET kind = EXCLUDED.kind, value = EXCLUDED.value"
)
FIELD_NAMES = ["id", "kind", "value", "timestamp"]


def get_source(brokers: str, topic: str) -> KafkaSource:
    return (KafkaSource.builder()
            .set_bootstrap_servers(brokers)
            .set_topics(topic)
            .set_group_id("kafka_postgres_group")
            .set_starting_offsets(KafkaOffsetsInitializer.latest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())


def get_sink():
    gateway = get_gateway()
    JsonJdbcSink = gateway.jvm.com.example.pyflink.sink.jdbc.JsonJdbcSink

    Properties = gateway.jvm.java.util.Properties

    conn_props = Properties()
    conn_props.setProperty("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    conn_props.setProperty("driver", "org.postgresql.Driver")
    conn_props.setProperty("username", POSTGRES_USER)
    conn_props.setProperty("password", POSTGRES_PASSWORD)

    exec_props = Properties()
    exec_props.setProperty("batch.interval.ms", "200")
    exec_props.setProperty("batch.size", "5")
    exec_props.setProperty("max.retries", "5")

    field_names = gateway.new_array(gateway.jvm.String, len(FIELD_NAMES))
    for i, name in enumerate(FIELD_NAMES):
        field_names[i] = name

    return JsonJdbcSink.getSink(SQL, field_names, conn_props, exec_props)


def run():
    get_gateway()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    if LOCAL_DEBUG:
        jar_location = str(path.join(path.dirname(path.abspath(__file__)), "../lib/bin/pyflink-services-1.0.jar"))
        env.add_jars(f"file:///{jar_location}")
        env.add_classpaths(f"file:///{jar_location}")

    stream = env.from_source(get_source(KAFKA_BROKERS, 'input_topic'),
                             WatermarkStrategy.no_watermarks(), "Kafka Source")

    stream = stream.filter(lambda record: record is not None and len(record.strip()) > 0)

    stream._j_data_stream.addSink(get_sink())

    env.execute("kafka-2-postgres")


if __name__ == '__main__':
    run()
