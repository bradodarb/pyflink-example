import os

from botocore.exceptions import ClientError

from iac.lib.aws_clients.boto3_clients import secrets
from iac.lib.logger.slog import slog


class SecretsManagerClient(object):
    def __init__(self, aws_profile: str = os.getenv('AWS_PROFILE', None),
                 region_name: str = os.getenv('AWS_REGION', None)):
        self.initial_aws_profile = os.getenv('AWS_PROFILE', None)
        self.aws_profile = aws_profile
        self.region_name = region_name

    def __getitem__(self, key):
        client = self._get_client()
        try:
            slog.info('Resolving SSM secret', key=key, profile=self.aws_profile, region=self.region_name)
            return client.get_secret_value(SecretId=key).get('SecretString')
        except ClientError as e:
            slog.error('Error while trying to read secret value', key=key, err=e)
            raise Exception(
                f'Error while trying to read secret value for key: {key} - {e.response["Error"]["Code"]}')
        finally:
            self._release_client()

    def __setitem__(self, key, value):
        # TODO - add ability to create/set secrets
        raise NotImplemented()

    def _get_client(self):
        os.environ['AWS_PROFILE'] = self.aws_profile
        return secrets(region_name=self.region_name)

    def _release_client(self):
        if self.initial_aws_profile is None:
            del os.environ['AWS_PROFILE']
        else:
            os.environ['AWS_PROFILE'] = self.initial_aws_profile
