from aws_cdk import aws_dynamodb as dynamodb, RemovalPolicy

def get_inference_setting_table(scope):
    construct_id = "inference-settings-Ref"
    return dynamodb.Table.from_table_name(
        scope=scope,
        id=construct_id,
        table_name="inference-settings"
    )