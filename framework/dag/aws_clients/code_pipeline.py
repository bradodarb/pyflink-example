import json
from typing import Dict, Optional

from framework.dag.aws_clients.boto3_clients import codepipeline
from framework.dag.logger.slog import slog


class PipelineExecutionResult:
    execution_id: Optional[str] = {}
    pipeline_name: str
    succeeded: Optional[bool]

    def __init__(self, pipeline_name: str, result: Dict = None):
        self.pipeline_name = pipeline_name
        if result and 'pipelineExecutionId' in result:
            self.execution_id = result['pipelineExecutionId']
            self.succeeded = True
            return
        self.execution_id = 'Failed'
        self.succeeded = False

    def to_dict(self):
        return {'pipeline_name': self.pipeline_name,
                'execution_id': self.execution_id,
                'succeeded': self.succeeded}

    def toJSON(self):
        return json.dumps(self.to_dict(), default=lambda o: {},
                          sort_keys=True)


def start_codepipeline(codepipeline_name: str, commit_hash: str) -> PipelineExecutionResult:
    """
    start AWS CodePipelines
    :param codepipeline_name: the CodePipelines to start
    :param commit_hash: the branch tag/commit/branch
    :return: dict holding the results of the start operations
    """
    codepipeline_client = codepipeline()
    try:
        slog.info('Starting Codepipeline Execution', projectName=codepipeline_name,
                  sourceVersion=commit_hash)
        response = codepipeline_client.start_pipeline_execution(
            name=codepipeline_name
        )
        slog.info(f'Started CodePipeline {codepipeline_name}.', execution=response)
        return PipelineExecutionResult(codepipeline_name, response)
    except codepipeline_client.exceptions.PipelineNotFoundException:
        slog.error(f'Could not find CodePipeline {codepipeline_name}.')
        return PipelineExecutionResult(codepipeline_name)
    except BaseException as err:
        slog.error(f'Error Starting CodePipeline {codepipeline_name}.', err=err)
        return PipelineExecutionResult(codepipeline_name)


def build_pipeline_url(region: str, pipeline_name: str) -> str:
    return f'https://{region}.console.aws.amazon.com/codepipeline/home?region={region}#/view/{pipeline_name}'
