#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk.cdk_infra_stack import CdkInfraStack
from cdk.cdk_stack import CdkStack


app = cdk.App()
infra = CdkInfraStack(app, "CdkInfraStack")
job = CdkStack(app, "CdkStack")
job.add_dependency(infra)

app.synth()
