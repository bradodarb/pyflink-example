import boto3

"""
We wrap the client providers here so we can provide reasonable defaults and so things are more testable
Since boto3 uses the same callable for all clients, it gets a little dubious to mock several types
"""


def s3(*args, **kwargs):
    return boto3.client('s3', *args, **kwargs)


def secrets(*args, **kwargs):
    return boto3.client('secretsmanager', *args, **kwargs)


def ssm(*args, **kwargs):
    return boto3.client('ssm', *args, **kwargs)


def codepipeline(*args, **kwargs):
    return boto3.client('codepipeline', *args, **kwargs)


def codebuild(*args, **kwargs):
    return boto3.client('codebuild', *args, **kwargs)


def eventbridge(*args, **kwargs):
    return boto3.client('events', *args, **kwargs)
