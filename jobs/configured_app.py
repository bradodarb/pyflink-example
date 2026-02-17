from typing import List

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import StreamTableEnvironment

from framework.dagger import Dag, FlowStep


def run():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])
    table_env.create_temporary_view("simple_source", table)
    print(table.get_schema())

    table_env.execute_sql("""
        CREATE TABLE first_sink_table (
            id BIGINT, 
            data VARCHAR 
        ) WITH (
            'connector' = 'print'
        )
    """)

    table_env.execute_sql("""
        CREATE TABLE second_sink_table (
            id BIGINT, 
            data VARCHAR
        ) WITH (
            'connector' = 'print'
        )
    """)

    # create a statement set
    statement_set = table_env.create_statement_set()

    # emit the "table" object to the "first_sink_table"
    statement_set.add_insert("first_sink_table", table)

    # emit the "simple_source" to the "second_sink_table" via a insert sql query
    statement_set.add_insert_sql("INSERT INTO second_sink_table SELECT * FROM simple_source")

    # execute the statement set
    statement_set.execute().wait()


class PyFlinkDag:
    def __init__(self, dag: Dag):
        self.dag = dag

        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.table_env = StreamTableEnvironment.create(self.env)

        self.sources = self._resolve_sources(dag.get_flow_step_sequence('source'))

        self.views = self._resolve_views(dag.get_flow_step_sequence('source'))

        self.sinks = self._resolve_sinks(dag.get_flow_step_sequence('sinks'))

    def _resolve_sources(self, steps: List[FlowStep]):
        pass

    def _resolve_views(self, steps):
        pass

    def _resolve_sinks(self, steps):
        pass


if __name__ == '__main__':
    class SourceStep(FlowStep):
        match_key = 'source'

        def _init(self):
            pass


    dag = Dag.load("../config/test_dag.yml")

    job = PyFlinkDag(dag)
    print(job.dag._name)
