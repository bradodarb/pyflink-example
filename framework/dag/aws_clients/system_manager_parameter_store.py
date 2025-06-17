from functools import lru_cache

from botocore.exceptions import ClientError

from iac.lib.aws_clients.boto3_clients import ssm
from iac.lib.logger.slog import slog


class SsmClient(object):

    @lru_cache(maxsize=None)
    def __getitem__(self, key):
        client = self._get_client()
        try:
            slog.info('Resolving SSM secret', key=key)
            return client.get_parameter(Name=key, WithDecryption=True).get('Parameter').get('Value')
        except ClientError as e:
            slog.error('Error while trying to read SSM value', key=key, err=e)
            if e.response["Error"]["Code"] == 'ParameterNotFound':
                slog.error(f'Key "{key}" not present in SSM')
                return None
            raise Exception(
                f'Error while trying to read SSM value for key: {key} - {e.response["Error"]["Code"]}')

    def __setitem__(self, key, value):
        # TODO - add ability to create/set params
        raise NotImplemented()

    @lru_cache(maxsize=None)
    def query(self, base_path: str):
        """
        Retrieve a set of parameters that share a common root pattern
        :param base_path: root path pattern to match on
        :return: list of parameter responses
        {
            'Parameters': [
                {
                    'Name': 'string',
                    'Type': 'String'|'StringList'|'SecureString',
                    'Value': 'string',
                    'Version': 123,
                    'Selector': 'string',
                    'SourceResult': 'string',
                    'LastModifiedDate': datetime(2015, 1, 1),
                    'ARN': 'string',
                    'DataType': 'string'
                },
            ],
            'NextToken': 'string'
        }
        """
        client = self._get_client()
        try:
            slog.info('Resolving SSM secrets', base_path=base_path)
            result = client.get_parameters_by_path(
                Path=base_path,
                Recursive=True,
                WithDecryption=True
            ).get('Parameters', [])

            result = {x['Name']: x['Value'] for x in result if x['Value']}
            if not len(result):  # handle case where base_path IS the param name!
                resolved_bas_path = self[base_path]
                if resolved_bas_path:
                    result = {base_path: resolved_bas_path}
            return result
        except ClientError as e:
            slog.error('Error while trying to read SSM value', base_path=base_path, err=e)
            if e.response["Error"]["Code"] == 'ParameterNotFound':
                slog.error(f'Keys under path "{base_path}" not present in SSM')
                return {}
            raise Exception(
                f'Error while trying to read SSM values for base_path: {base_path} - {e.response["Error"]["Code"]}')

    def _get_client(self):
        return ssm()


if __name__ == '__main__':
    client = SsmClient()
    items = client.query('/pipeline/action/cnn-dataviz-apis-marketdata/ci__develop__ci')
    slog.info('got params', items)
