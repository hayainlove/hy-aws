import aws_cdk as core
import aws_cdk.assertions as assertions

from my_hayati_phase2.my_hayati_phase2_stack import MyHayatiPhase2Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in my_hayati_phase2/my_hayati_phase2_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = MyHayatiPhase2Stack(app, "my-hayati-phase2")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
