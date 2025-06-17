import json
from typing import List, Dict, Optional

from iac.lib.aws_clients.boto3_clients import codebuild
from iac.lib.logger.slog import slog


class BuildExecutionResult:
    execution_id: Optional[str] = {}
    pipeline_name: str
    succeeded: Optional[bool]

    def __init__(self, pipeline_name: str, result: Dict = None):
        self.pipeline_name = pipeline_name
        if result and 'build' in result:
            self.execution_id = result['build']['id']
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


def start_build(codebuild_name: str, commit_hash: str) -> BuildExecutionResult:
    """
    start AWS CodePipelines
    :param codebuild_name: the CodePipelines to start
    :param commit_hash: the branch tag/commit/branch
    :return: dict holding the results of the start operations
    """

    codebuild_client = codebuild()
    try:
        slog.info('Starting Codebuild Execution', projectName=codebuild_name,
                  sourceVersion=commit_hash)
        response = codebuild_client.start_build(
            projectName=codebuild_name,
            sourceVersion=commit_hash
        )
        slog.info(f'Started CodeBuild {codebuild_name}.', execution=response)
        return BuildExecutionResult(codebuild_name, response)
    except codebuild_client.exceptions.InvalidInputException as err:
        slog.error(f'Invalid Codebuild Execution Params {codebuild_name}.', err=err)
        return BuildExecutionResult(codebuild_name)
    except codebuild_client.exceptions.ResourceNotFoundException as err:
        slog.error(f'Could not find CodeBuild project {codebuild_name}.', err=err)
        return BuildExecutionResult(codebuild_name)
    except codebuild_client.exceptions.ResourceNotFoundException as err:
        slog.error('CodeBuild Execution Limit Exceeded.', err=err)
        return BuildExecutionResult(codebuild_name)
    except BaseException as err:
        slog.error(f'Error Starting CodePipeline {codebuild_name}.', err=err)
        return BuildExecutionResult(codebuild_name)


ALL_CONTRACT_TARGETS = ['deps', 'clean', 'lint', 'type_check',
                        'integration_test', 'unit_test', 'build',
                        'deploy', 'e2e', 'remove', 'apply']


def get_contract_build_spec(base_path: str, runtime_def: Dict = None, targets: List[str] = []):
    if not runtime_def:
        runtime_def = {
            'python': 3.8
        }
    target_phase_map = {
        'ci_deps': 'install',
        'deps': 'install',
        'clean': 'install',
        'type_check': 'pre_build',
        'lint': 'pre_build',
        'unit_test': 'build',
        'build': 'build',
        'build_stack': 'build',
        'build_stacks': 'build',
        'build_deploy': 'build',
        'build_deploy_stack': 'build',
        'deploy': 'build',
        'deploy_stacks': 'build',
        'ci_deploy': 'build',
        'cd_deploy': 'build',
        'integration_test': 'build',
        'ci_integration_test': 'build',
        'e2e': 'build',
        'remove': 'post_build',
        'destroy': 'post_build',
        'destroy_stacks': 'post_build',
        'ci_destroy': 'post_build',
        'cd_destroy': 'post_build',
        'plan': 'post_build'
    }
    target_ordinality_map = {
        'ci_deps': -1,
        'deps': 0,
        'clean': 1,
        'lint': 2,
        'type_check': 3,
        'unit_test': 4,
        'build': 5,
        'build_stack': 5,
        'build_stacks': 5,
        'build_deploy': 5,
        'build_deploy_stack': 5,
        'deploy_stacks': 6,
        'cd_deploy': 6,
        'ci_deploy': 6,
        'deploy': 6,
        'integration_test': 7,
        'ci_integration_test': 7,
        'e2e': 8,
        'remove': 9,
        'destroy': 9,
        'destroy_stacks': 9,
        'ci_destroy': 9,
        'cd_destroy': 9,
        'plan': 9
    }
    key_order = list(sorted(target_ordinality_map, key=target_ordinality_map.get))
    ops = list(sorted(targets, key=key_order.index))

    template = {
        'version': 0.2,
        'env': {
            'git-credential-helper': 'yes'
        },
        'phases': {
            'install': {'runtime-versions': runtime_def, 'commands': ['env', 'ls', 'pwd', f'cd {base_path}',
                                                                      'pip install --upgrade pip setuptools wheel']},
            'pre_build': {'commands': []},
            'build': {'commands': []},
            'post_build': {'commands': []},
        }}
    phases = template['phases'].keys()
    for phase in phases:
        template['phases'][phase]['commands'].extend([f'make {op}' for op in ops if target_phase_map.get(op) == phase])

    return template
