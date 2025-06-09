import boto3
import json
import logging
import os

s3_client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    # bucket_name = "cache-us-east-1-054037105643-15bd31e070bd"
    
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
    
    bucket_name = cache_bucket

    # Extract videoId and executionUUID from path parameters
    video_id = event.get("pathParameters", {}).get("videoId")
    execution_UUID = event.get("pathParameters", {}).get("executionId")
    
    if not video_id or not execution_UUID:
        print(f"Error: Missing videoId or executionUUID (videoId={video_id}, executionUUID={execution_UUID})")
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            "body": json.dumps({"error": "videoId or executionUUID is missing"})
        }

    # Construct S3 folder path
    folder_prefix = f"batch-videos/{video_id}/{execution_UUID}/"
    chunks_prefix = f"{folder_prefix}chunks/"
    print(f"Listing objects in s3://{bucket_name}/{folder_prefix}")

    try:
        # Check if the folder exists
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix, MaxKeys=1)
        print(f"List objects response: {response}")
        
        if "Contents" not in response:
            print(f"No folder found: {folder_prefix}")
            return {
                "statusCode": 404,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,GET"
                },
                "body": json.dumps({
                    "data": {
                        "status": "NO_SUCH_EXECUTION",
                        "videoId": video_id,
                        "executionId": execution_UUID
                    }
                })
            }

        # Check for chunks folder and contents
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=chunks_prefix)
        print(f"List chunks response: {response}")
        
        if "Contents" not in response:
            print(f"No chunks folder or contents found: {chunks_prefix}")
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,GET"
                },
                "body": json.dumps({
                    "data": {
                        "status": "RUNNING",
                        "videoId": video_id,
                        "executionId": execution_UUID
                    }
                })
            }

        # Collect .mp4 and .json files
        mp4_files = []
        json_files = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".mp4"):
                mp4_files.append(key)
                print(f"Found .mp4 file: {key}")
            elif key.endswith(".json"):
                json_files.append(key)
                print(f"Found .json file: {key}")

        if not mp4_files:
            print(f"No .mp4 files found in: {chunks_prefix}")
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,GET"
                },
                "body": json.dumps({
                    "data": {
                        "status": "RUNNING",
                        "videoId": video_id,
                        "executionId": execution_UUID
                    }
                })
            }

        # Check for matching .json files
        mp4_base_names = {key.rsplit("/", 1)[-1].rsplit(".", 1)[0] for key in mp4_files}
        json_base_names = {key.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace("ts_", "", 1) for key in json_files}
        missing_json = mp4_base_names - json_base_names
        
        if missing_json:
            print(f"Missing .json files for .mp4 files: {missing_json}")
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,GET"
                },
                "body": json.dumps({
                    "data": {
                        "status": "RUNNING",
                        "videoId": video_id,
                        "executionId": execution_UUID
                    }
                })
            }

        # Check each .json file for errors
        status = "SUCCEEDED"
        for json_file in json_files:
            print(f"Fetching .json file: s3://{bucket_name}/{json_file}")
            try:
                s3_response = s3_client.get_object(Bucket=bucket_name, Key=json_file)
                json_content = s3_response["Body"].read().decode("utf-8")
                json_data = json.loads(json_content)
                print(f"JSON content: {json_content[:100]}...")
                
                # Check for Internal Server Error in time-based keys or vllm.result
                for key, value in json_data.items():
                    # Case 1: Direct string value is "Internal Server Error"
                    if isinstance(value, str) and value == "Internal Server Error":
                        status = "FAILED"
                        print(f"Found Internal Server Error in key {key} of {json_file}, setting status: {status}")
                        break
                    # Case 2: vllm.result array contains "Internal Server Error"
                    elif isinstance(value, dict):
                        vllm = value.get("vllm", {})
                        if isinstance(vllm, dict):  # Ensure vllm is a dict before accessing result
                            vllm_result = vllm.get("result", [])
                            if isinstance(vllm_result, list) and "Internal Server Error" in vllm_result:
                                status = "FAILED"
                                print(f"Found Internal Server Error in vllm.result of {json_file}, setting status: {status}")
                                break
                        # If vllm is a string, skip without error
                if status == "FAILED":
                    break

            except Exception as e:
                print(f"Error reading .json file {json_file}: {str(e)}")
                return {
                    "statusCode": 500,
                    "headers": {
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Methods": "OPTIONS,GET"
                    },
                    "body": json.dumps({"error": f"Failed to read transcript: {str(e)}"})
                }

        print(f"Determined status: {status}")
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            "body": json.dumps({
                "data": {
                    "status": status,
                    "videoId": video_id,
                    "executionId": execution_UUID
                }
            })
        }

    except Exception as e:
        print(f"Error listing objects in folder {folder_prefix}: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            "body": json.dumps({"error": f"Failed to fetch files: {str(e)}"})
        }