#!/usr/bin/env python3
import aws_cdk as cdk
from my_hayati_phase2.my_hayati_phase2_stack import MyHayatiPhase2Stack

app = cdk.App()

# Get alarm email from context or use default
alarm_email = app.node.try_get_context("alarm_email") or "your.email@example.com"

MyHayatiPhase2Stack(
    app,
    "MyHayatiPhase2Stack",
    alarm_email=alarm_email,
    # env parameter removed - CDK will use your default AWS credentials
)

app.synth()