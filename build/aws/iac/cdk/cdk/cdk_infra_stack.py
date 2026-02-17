from aws_cdk import (
    # Duration,
    Stack,
    aws_s3, RemovalPolicy,
    aws_iam
)
from constructs import Construct


class CdkInfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        bucket = aws_s3.Bucket(self, 'iac-cdk-bucket',
                               bucket_name='pyflink-bucket',
                               removal_policy=RemovalPolicy.DESTROY)

        bucket.add_to_resource_policy(aws_iam.PolicyStatement(effect=aws_iam.Effect.DENY,
                                                              actions=["s3:*"],
                                                              resources=["arn:aws:s3:::pyflink-bucket",
                                                                         "arn:aws:s3:::pyflink-bucket/*"],
                                                              principals=[aws_iam.AnyPrincipal()],
                                                              conditions={
                                                                  "Bool": {
                                                                      "aws:SecureTransport": "false"
                                                                  }
                                                              }))
