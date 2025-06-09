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
    test_batch_video_chat_lambda_function,
    test_batch_video_execution_lambda_function,
    test_batch_video_transcript_lambda_function,
    test_get_status_by_id_lambda_function
)

# API Gateway
from stack.api_gateway import (
    build_batch_chat_testing_api_gateway
)

# Tables
from stack.table import (
    get_inference_setting_table
)

class BatchTestingCdkStack(Stack):

    def __init__(self, scope, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        #role
        lambda_role = create_lambda_role(self)
        
        # Create the Layer once
        pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self, "AWSSDKPandasLayer",
            "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:16"
        )
        
        #tables
        inference_table=get_inference_setting_table(self)

        
        #actual lambda called by lambda function
        batch_video_chat_test_lambda= test_batch_video_chat_lambda_function(self,"BatchVideTestChatLambda", "batch-video-chat-testing", lambda_role, inference_table)
        batch_video_execution_test_lambda = test_batch_video_execution_lambda_function(self,"BatchVideoTestExecutionLambda", "batch-video-execution-testing", lambda_role, pandas_layer, inference_table)
        batch_video_transcript_test_lambda= test_batch_video_transcript_lambda_function(self, "BatchVideoTestTranscriptLambda", "batch-video-transcript-testing",lambda_role, inference_table )
        batch_video_get_status_by_id_test_lambda = test_get_status_by_id_lambda_function(self, "BatchVideoGetStatusByIdTestLambda", "batch-video-get-status-by-id-test", lambda_role, inference_table )
        
        #lambda attached to apigateway
        api= build_batch_chat_testing_api_gateway(
            self,
            batch_video_chat_test_lambda,
            batch_video_execution_test_lambda,
            batch_video_transcript_test_lambda,
            batch_video_get_status_by_id_test_lambda
        )
         
        CfnOutput(self, "BatchVideoTestUrl", value=api.url)
         
