import json
import os
import re
from functools import lru_cache
from typing import List

import yaml

from lib.aws_clients.secrets_manager import SecretsManagerClient
from lib.aws_clients.system_manager_parameter_store import SsmClient

SSM_PREFIX = 'ssm:'
SECRET_MANAGER_PREFIX = 'secret:'
ENV_VAR_PREFIX = 'env:'
FILE_PREFIX = 'file:'

YAML_EXTENSIONS = ['.yml', '.yaml']
JSON_EXTENSION = '.json'

PIPELINE_FILE = 'pipeline.yml'

ssm = SsmClient()
class PipelineConfigLoader:

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.loader = yaml.SafeLoader
        self.token_pattern = re.compile('.*?\${(.*?)}.*?')
        self.ssm_client = ssm
        self.secret_client = SecretsManagerClient()

        def resolver(loader, node):
            value = loader.construct_scalar(node)
            match = self.token_pattern.findall(value)
            if match:
                full_value = value
                for g in match:
                    resolved_value = g
                    if self.is_env(g):
                        resolved_value = self.resolve_env(g)
                    elif self.is_ssm(g):
                        resolved_value = self.resolve_ssm(g)
                    elif self.is_secret(g):
                        resolved_value = self.resolve_secret(g)
                    elif self.is_file(g):
                        return self.resolve_file(g)
                    full_value = full_value.replace(
                        f'${{{g}}}', resolved_value
                    )
                return full_value
            return value

        self.loader.add_implicit_resolver('!REF', self.token_pattern, None)
        self.loader.add_constructor('!REF', resolver)

    def load(self, path: str):
        resolved_path = os.path.join(self.base_path, path)
        with open(resolved_path, 'r') as conf_data:
            return yaml.load(conf_data, Loader=self.loader)

    @lru_cache(maxsize=None)
    def load_json(self, path: str):
        resolved_path = os.path.join(self.base_path, path)
        with open(resolved_path, 'r') as conf_data:
            return json.loads(conf_data.read())

    @classmethod
    def is_ssm(cls, token: str) -> bool:
        return token.startswith(SSM_PREFIX)

    @classmethod
    def is_secret(cls, token: str) -> bool:
        return token.startswith(SECRET_MANAGER_PREFIX)

    @classmethod
    def is_env(cls, token: str) -> bool:
        return token.startswith(ENV_VAR_PREFIX)

    @classmethod
    def is_file(cls, token: str) -> bool:
        return token.startswith(FILE_PREFIX)

    @lru_cache(maxsize=None)
    def resolve_ssm(self, token: str) -> str:
        key = token.lstrip(SSM_PREFIX)
        if key:
            return self.ssm_client[key]
        return token

    @lru_cache(maxsize=None)
    def resolve_secret(self, token: str) -> str:
        key = token.lstrip(SECRET_MANAGER_PREFIX)
        if key:
            return self.secret_client[key]
        return token

    @lru_cache(maxsize=None)
    def resolve_env(self, token: str) -> str:
        key = token.lstrip(ENV_VAR_PREFIX)
        if key:
            return os.environ.get(key, key)
        return token

    @lru_cache(maxsize=None)
    def resolve_file(self, token: str) -> str:
        key = token.lstrip(FILE_PREFIX)
        if key:
            if len([key.endswith(yml) for yml in YAML_EXTENSIONS]) > 0:
                return self.load(key)
            elif key.endswith(JSON_EXTENSION):
                return self.load_json(key)
        return token


def resolve(value):
    loader = PipelineConfigLoader(os.path.join(os.path.curdir, value))
    return loader.load(PIPELINE_FILE)


def gather_folders(base_path: str) -> List[str]:
    root_path = os.path.join(os.path.dirname(__file__), '../', base_path)

    result = [folder.path for folder in
              os.scandir(root_path) if
              folder.is_dir()]
    print(result)
    return result


def load_pipelines(base_path: str):
    print("base path "+base_path)
    pipelines = [PipelineConfigLoader(os.path.join(os.path.curdir, base_path)).load(PIPELINE_FILE)]
    return pipelines


if __name__ == '__main__':
    print(resolve('../stacks/chatops-demo'))
