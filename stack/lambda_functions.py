from aws_cdk import Duration, aws_lambda as _lambda, aws_iam as iam, aws_cognito as cognito
import aws_cdk as cdk
import os


def test_batch_video_chat_lambda_function(scope, function_name, handler_file,  lambda_role, table):
    return _lambda.Function(
        scope, function_name,
        runtime=_lambda.Runtime.PYTHON_3_12,
        handler=f"{handler_file}.handler",
        code=_lambda.Code.from_asset(os.path.join(os.getcwd(), 'lambda', handler_file)),
        role=lambda_role,
        environment={
            'INFERENCE_SETTINGS_TABLE_NAME': table.table_name
        },
        timeout=Duration.minutes(5),
    )
    
def test_batch_video_execution_lambda_function(scope, function_name, handler_file, lambda_role, layer, table):
    return _lambda.Function(
        scope, function_name,
        runtime=_lambda.Runtime.PYTHON_3_12,
        handler=f"{handler_file}.handler",
        code=_lambda.Code.from_asset(os.path.join(os.getcwd(), 'lambda', handler_file)),
        role=lambda_role,
        timeout=Duration.minutes(5),
        environment={
            'INFERENCE_SETTINGS_TABLE_NAME': table.table_name
        },
        layers=[layer]
    )
    
def test_batch_video_transcript_lambda_function(scope, function_name, handler_file, lambda_role, table):
    return _lambda.Function(
        scope, function_name,
        runtime=_lambda.Runtime.PYTHON_3_12,
        handler=f"{handler_file}.handler",
        code=_lambda.Code.from_asset(os.path.join(os.getcwd(), 'lambda', handler_file)),
        role=lambda_role,
        timeout=Duration.minutes(5),
        environment={
            'INFERENCE_SETTINGS_TABLE_NAME': table.table_name
        },
    )
    
def test_get_status_by_id_lambda_function(scope, function_name, handler_file, lambda_role, table):
    return _lambda.Function(
        scope, function_name,
        runtime=_lambda.Runtime.PYTHON_3_12,
        handler=f"{handler_file}.handler",
        code=_lambda.Code.from_asset(os.path.join(os.getcwd(), 'lambda', handler_file)),
        environment={
            'INFERENCE_SETTINGS_TABLE_NAME': table.table_name
        },
        role=lambda_role,
        timeout=Duration.minutes(1)
    )
    
def test_events_lambda_function(scope, function_name, handler_file, lambda_role):
    return _lambda.Function(
        scope, function_name,
        runtime=_lambda.Runtime.PYTHON_3_12,
        handler=f"{handler_file}.handler",
        code=_lambda.Code.from_asset(os.path.join(os.getcwd(), 'lambda', handler_file)),
        role=lambda_role,
        timeout=Duration.minutes(1)
    )
    

    
def create_lambda_role(scope):
    lambda_role = iam.Role(
        scope, "LambdaExecutionRole",
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonCognitoPowerUser"),
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonDynamoDBFullAccess"),
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisVideoStreamsFullAccess"),
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonBedrockFullAccess"),
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess")
        ],
        inline_policies={
            "BedrockAndLoggingPolicy": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        actions=["logs:CreateLogGroup"],
                        resources=["arn:aws:logs:us-east-1:054037105643:*"],
                        effect=iam.Effect.ALLOW
                    ),
                    iam.PolicyStatement(
                        actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                        resources=["arn:aws:logs:us-east-1:054037105643:log-group:/aws/lambda/step_4_chat_with_bedrock:*"],
                        effect=iam.Effect.ALLOW
                    ),
                    iam.PolicyStatement(
                        actions=["bedrock:InvokeAgent", "bedrock:InvokeModel"],
                        resources=["*"],
                        effect=iam.Effect.ALLOW
                    ),
                    iam.PolicyStatement(
                        actions=["s3:ListBucket", "s3:GetObject", "s3:PutObject"],
                        resources=[
                            "arn:aws:s3:::cctvfootageap",
                            "arn:aws:s3:::cctvfootageap/*",
                            "arn:aws:s3:::transcriptsmumbai",
                            "arn:aws:s3:::transcriptsmumbai/*",
                            "arn:aws:s3:::accident-alert-bucket-us-east-1",
                            "arn:aws:s3:::accident-alert-bucket-us-east-1/*",
                            "arn:aws:s3:::testing-video-and-transcript-us-east-2-337909749314/artifacts/event_detection/events_to_detect.json",
                            "arn:aws:s3:::testing-video-and-transcript-us-east-2-337909749314"
                        ],
                        effect=iam.Effect.ALLOW
                    ),
                    iam.PolicyStatement(
                        actions=["sns:Publish"],
                        resources=["arn:aws:sns:us-east-1:054037105643:AccidentAlertTopic"],
                        effect=iam.Effect.ALLOW
                    )
                ]
            )
        }
    )
    return lambda_role