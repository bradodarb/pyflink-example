import aws_cdk.aws_kinesisanalytics_flink_alpha as flink
from aws_cdk import (
    # Duration,
    Stack,
    aws_s3, RemovalPolicy

)
from constructs import Construct


class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = aws_s3.Bucket(self, 'iac-cdk-bucket',
                               bucket_name='pyflink-bucket',
                               removal_policy=RemovalPolicy.RETAIN)

        app = flink.Application(self, 'app',
                                runtime=flink.Runtime.FLINK_1_19,
                                code=flink.ApplicationCode.from_bucket(bucket, 'releases/pyflink-example.zip'))
