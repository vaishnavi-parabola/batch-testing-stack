import json
import boto3
import os
import logging

S3 = boto3.client('s3')
BUCKET = "spectracdkstack-batchvideobucketa35fe309-p3omgtksdngd"
KEY = "artifacts/event_detection/events_to_detect.json"

def handler(event, context):
    method = event.get('httpMethod')
    if not method:
         return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                'body': json.dumps({'error':  'Invalid event structure'})
            }
    if method == 'GET':
        try:
            resp = S3.get_object(Bucket=BUCKET, Key=KEY)
            data = resp['Body'].read().decode('utf-8')
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                'body': data
            }
        except S3.exceptions.NoSuchKey:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                'body': json.dumps({'error':  'Events file not found'})
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST'
                },
                'body': json.dumps({'error':  f'Failed to retrieve events: {str(e)}'})
            }
    if method == 'PUT':
        # Define CORS headers
        cors_headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET,PUT'
        }
        try:
            body = json.loads(event.get('body', '{}'))
            logging.info("Parsed body: %s", json.dumps(body))
            if 'events' not in body or not isinstance(body['events'], list):
                logging.error("Invalid payload: events must be a list")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Invalid payload: events must be a list'})
                }
            # Validate that all elements in the events array are strings
            if not all(isinstance(item, str) for item in body['events']):
                logging.error("Invalid payload: all events must be strings")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'error': 'Invalid payload: all events must be strings'})
                }
            S3.put_object(
                Bucket=BUCKET,
                Key=KEY,
                Body=json.dumps({'events': body['events']}),
                ContentType='application/json'
            )
            logging.info("Successfully updated S3 object")
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Updated'})
            }
        except json.JSONDecodeError:
            logging.error("Invalid JSON payload")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid JSON payload'})
            }
        except Exception as e:
            logging.error("Failed to update events: %s", str(e))
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Failed to update events: {str(e)}'})
            }
    return {
        'statusCode': 405,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT'
        },
        'body': json.dumps({'error': f'Failed to update events: {str(e)}'})
    }