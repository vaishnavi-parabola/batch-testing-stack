from aws_cdk import Stack, CfnOutput, Duration
from aws_cdk import aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
import aws_cdk.aws_s3_notifications as s3_notifications
from aws_cdk import aws_sns as sns
import aws_cdk as cdk


from stack.lambda_functions import (
    create_lambda_role,
    test_batch_video_chat_lambda_function
)

# API Gateway
from stack.api_gateway import (
    build_batch_chat_testing_api_gateway
)

class BatchTestingCdkStack(Stack):

    def __init__(self, scope, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        #role
        lambda_role = create_lambda_role(self)
        
        #actual lambda called by lambda function
        batch_video_chat_test_lambda= test_batch_video_chat_lambda_function(self,"BatchVideTestChatLambda", "batch-video-chat-testing", lambda_role)
        
        #lambda attached to apigateway
        api= build_batch_chat_testing_api_gateway(
            self,
            batch_video_chat_test_lambda
        )
         
        CfnOutput(self, "BatchVideoTestUrl", value=api.url)
         
