import json
import uuid
import requests
import boto3
import logging
import os

# Set up logging
logging.getLogger().setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')

def handler(event, context):
    try:
        logging.info("Received event: %s", json.dumps(event))
        
        # Extract input data from the event body
        input_data = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', {})
        logging.info("Parsed input_data: %s", json.dumps(input_data))
        
        # Generate runtime prefix
        runtime_prefix = str(uuid.uuid4())
        logging.info("Generated runtime_prefix: %s", runtime_prefix)
        
        # Update s3_dest_uri_w_prefix with runtime prefix
        if 's3_dest_uri_w_prefix' in input_data:
            s3_uri = input_data['s3_dest_uri_w_prefix']
            if isinstance(s3_uri, str) and '{runtime_prefix}' in s3_uri:
                try:
                    input_data['s3_dest_uri_w_prefix'] = s3_uri.format(runtime_prefix=runtime_prefix)
                    logging.info("Updated s3_dest_uri_w_prefix: %s", input_data['s3_dest_uri_w_prefix'])
                except ValueError as e:
                    logging.error("String formatting error for s3_dest_uri_w_prefix: %s", str(e))
                    raise Exception(f"Invalid s3_dest_uri_w_prefix format: {s3_uri}")
            else:
                logging.warning("s3_dest_uri_w_prefix lacks {runtime_prefix} or is not a string, skipping formatting: %s", s3_uri)
                # Optionally append runtime_prefix if needed
                # input_data['s3_dest_uri_w_prefix'] = f"{s3_uri.rstrip('/')}/{runtime_prefix}/"
        
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
        inference_endpoint = item.get('inference_endpoint')
        if not inference_endpoint:
            raise Exception("inference_endpoint not found in item")
        
        logging.info("process_video endpoint: %s", inference_endpoint)
        
        # Make POST request to the specified URL
        logging.info("Sending POST request to process_video endpoint")
        response = requests.post(
            inference_endpoint,
            json=input_data,
            timeout=60
        )
        response.raise_for_status()
        logging.info("Received response from process_video: %s", response.text)
        
        return {
            'statusCode': response.status_code,
            'body': json.dumps(response.json()),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }
    
    except requests.exceptions.RequestException as e:
        logging.error("Error in POST request: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Failed to process video: {str(e)}"}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }
    except Exception as e:
        logging.error("Unexpected error: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Unexpected error: {str(e)}"}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': 'true'
            }
        }