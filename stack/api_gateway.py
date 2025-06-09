import aws_cdk as cdk
from aws_cdk import (
    aws_apigateway as apigateway,
    aws_iam as iam
)

def build_batch_chat_testing_api_gateway(scope, batch_video_chat_test_lambda, batch_video_execution_test_lambda, batch_video_transcript_test_lambda, batch_video_get_status_by_id_test_lambda):
    api = apigateway.RestApi(
        scope, "BatchChatTestingAPI",
        rest_api_name="BatchChatTesting API",
        default_cors_preflight_options=apigateway.CorsOptions(
            allow_origins=apigateway.Cors.ALL_ORIGINS,
            allow_headers=["Content-Type", "X-Amz-Date", "Authorization", "X-Api-Key"],
            allow_methods=["OPTIONS", "POST", "GET", "PUT", "DELETE"]
        )
    )
    
    #lambda attached to api gateway
    api.root.add_resource("batch-video-chat-test").add_method("POST", apigateway.LambdaIntegration(batch_video_chat_test_lambda))
    api.root.add_resource("batch-video-execution-test").add_method("POST", apigateway.LambdaIntegration(batch_video_execution_test_lambda))
    api.root.add_resource("batch-video-transcript-test").add_method("POST", apigateway.LambdaIntegration(batch_video_transcript_test_lambda))
    videos=api.root.add_resource("videos")
    video_id = videos.add_resource("{videoId}")
    videos = video_id.add_resource("executions")
    execution_uuid = videos.add_resource("{executionId}")
    status = execution_uuid.add_resource("status-test")
    status.add_method(
        "GET",
        apigateway.LambdaIntegration(batch_video_get_status_by_id_test_lambda),    
    )

    
    return api
    



