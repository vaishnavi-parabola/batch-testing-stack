import json
import boto3
import os
from decimal import Decimal
import logging

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# DEST_BUCKET = 'cache-us-east-1-054037105643-15bd31e070bd'

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def list_transcript_files(bucket, prefix):
    """List all JSON transcript files in the specified S3 prefix."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [
        item['Key'] for item in response.get('Contents', [])
        if item['Key'].endswith('.json') and 'chunk_start' in item['Key']
    ]

def merge_transcripts(bucket, keys):
    """Merge transcript files from S3 into a single response."""
    results = []
    for key in sorted(keys):
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read().decode('utf-8')
        try:
            data = json.loads(content)
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except json.JSONDecodeError as e:
            print(f"⚠️ Skipping invalid JSON in {key}: {e}")

    # Create final format
    merged_output = {
        "statusCode": 200,
        "videoTranscript": {
            "results": json.dumps(results),
            "count_results": []
        },
        "totalInferenceTimeinSecs": {
            "ray_processing_time_in_secs": "11.34",
            "remote_inference_time_in_secs": "23.08"
        },
        "InferenceParams": {
            "min_threshold": 0.4,
            "min_threshold_step_inc": 0.1,
            "user_prompt": "Describe the video in great detail. Keep an eye out any accidents or incidents, Always capture the location, times if available in the watermarks with every response.",
            "system_prompt_s3uri": "s3://dev-testing-video-and-transcript-us-east-1-054037105643/artifacts/reference-prompts/system_prompt_pranav_dev.json",
            "video_frames": {
                "width": 500,
                "height": 400
            },
            "inference_configuration": {
                "akash_inference_endpoint": "http://provider.a100.hou3.dd.akash.pub:30743/invocations",
                "akash_yolo_endpoint": "http://provider.akashprovid.com:30635/",
                "runpod_api_key": None,
                "runpod_endpoint": None,
                "max_concurrency": 4
            },
            "properties": {
                "temperature": 0.1,
                "top_p": 0.001,
                "repetition_penalty": 1.05,
                "max_tokens": 18000,
                "stop_token_ids": []
            },
            "chunk_duration_in_secs": 60,
        },
        "userInformation": {
            "UploadVideoComments": "This is a test video",
            "UploadVideoDatetime": "2025-01-05 03:28:49.113283",
            "VideoMetaData": {
                "SourceIp": "127.0.0.1",
                "RequestOrigin": {
                    "DeviceType": "web",
                    "Browser": "Chrome 96",
                    "OS": "Windows 11"
                }
            },
            "UserHash": "87dd780d119022fb9bae47a0d6e74ca517dd0576"
        },
        "sessionInformation": {
            "SessionID": "session-abc-12345",
            "ApiToken": "abcdef123456",
            "SessionStartTime": "2025-01-05 03:28:49.113303",
            "SessionEndTime": ""
        }
    }
    return merged_output

def handler(event, context):
    """Handle POST request to retrieve merged transcript for a videoId."""
    try:
        # Parse the request body
        body = json.loads(event.get('body', '{}'))
        video_id = body.get('videoId')
        execution_uuid = body.get('executionUUID')  # Optional, if needed

        if not video_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST'
                },
                'body': json.dumps({'error': 'videoId is required'})
            }

        # Construct the S3 prefix for transcripts
        prefix = f"batch-videos/{video_id}/{execution_uuid}/chunks/" if execution_uuid else f"batch-videos/{video_id}/chunks/"
        
        table_name = os.environ.get('INFERENCE_SETTINGS_TABLE_NAME', 'inference-settings')
        logging.info("Using DynamoDB table: %s", table_name)
        table = dynamodb.Table(table_name)
        
        # Check if the video entry exists in DynamoDB
        try:
            inference_record = table.get_item(
                Key={'inference_setting_id': '1'}  # Partition key is a string
            )
        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            logging.error("DynamoDB table %s does not exist", table_name)
            raise Exception(f"DynamoDB table {table_name} does not exist")
        
        item = inference_record.get('Item')
        if not item:
            raise Exception("Inference setting not found for inference_setting_id: 1")
        cache_bucket = item.get('cache_bucket')
        if not cache_bucket:
            raise Exception("cache_bucket not found in item")
        
        logging.info("cache_bucket: %s", cache_bucket)
        
        DEST_BUCKET=cache_bucket

        # List transcript files
        transcript_keys = list_transcript_files(DEST_BUCKET, prefix)

        if not transcript_keys:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST'
                },
                'body': json.dumps({'error': 'No transcript files found for the given videoId'})
            }

        # Merge transcripts
        merged_transcript = merge_transcripts(DEST_BUCKET, transcript_keys)

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': json.dumps({
                'videoId': video_id,
                'transcript': merged_transcript
            }, cls=DecimalEncoder)
        }

    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': json.dumps({'error': 'Invalid JSON in request body'})
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST'
            },
            'body': json.dumps({'error': str(e)})
        }