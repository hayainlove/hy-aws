import aws_cdk as cdk
import sys
import os

# Add the project directory to Python path
sys.path.append(os.path.dirname(__file__))

from my_hayati_phase2.application_stack import MyHayatiApplicationStack
from my_hayati_phase2.infrastructure_stack import MyHayatiInfrastructureStack

app = cdk.App()

# Get alarm email from context or use default
alarm_email = app.node.try_get_context("alarm_email") or "mt-nurhayatihasbi@axrail.com"

# Deploy infrastructure stack first
infra_stack = MyHayatiInfrastructureStack(
    app,
    "MyHayatiInfrastructureStack",
    alarm_email=alarm_email,
)

# Deploy application stack second (depends on infrastructure)
app_stack = MyHayatiApplicationStack(
    app,
    "MyHayatiApplicationStack",
    infra_stack=infra_stack,
)

# Ensure application stack depends on infrastructure stack
app_stack.add_dependency(infra_stack)

app.synth()