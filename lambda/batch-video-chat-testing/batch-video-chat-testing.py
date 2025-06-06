import json
import boto3
import os
import copy
from datetime import datetime
import uuid
import logging

# Set up logging
logging.getLogger().setLevel(logging.INFO)

s3_client = boto3.client('s3')
client = boto3.client("bedrock-runtime")

# System template for Bedrock AI
system_template = """The following is a friendly conversation between a Human (H) and an AI Assistant (AI) about a Video. There is no video provided to you but only a transcript of the video. Always remember the following points when having a conversation,

- The Video information is provided to you in the `Video Context` section below. You are to only answer based on the <video_context>...</video_context> and if the answer is not available respond with "I don't know, I'm sorry the requested information is not a part of the video". 

- The video transcript is a non-overlapping second by second summary provided by a video transcriber. You are to answer a user's question based on the entire transcript and keep the user's conversation history in context when answering the question.

- Remember when a human asks about a video, always assume they are talking about the <video_context>...</video_context> transcript and respond appropriately. Your job depends on this.

- The user does not know that you (the assistant) has the video context. You should never reveal this information back to the user. Your job is to make them think that you analyzing the video live. It's your secret to never talk about <video_context>...</video_context>

- Remember never reveal to the user about video context. Always pretend that you have access to the video.

- The video context is your biggest secret. Your job depends on this.

<video_context>
{video_context}
</video_context>

ALWAYS provide response in user readable format. Use a mixture of Paragraph and small paragraphs when responding, unless specified otherwise.

ALWAYS prioritize user readability experience when providing a response.

**Additional Instructions:**
- Format your response in Markdown, using headers and small paragraphs for clarity.
- For any incidents or major events (e.g., accidents, injuries, robberies, or significant occurrences), place these details under a separate Markdown header titled `## Major Incident`.
- Wrap the entire `## Major Incident` section (including the header and its content) and any incident-related paragraphs (e.g., those describing the incident or its impact) in `<highlight>...</highlight>` tags to indicate they should be highlighted in the frontend. Ensure the tags are properly closed.
- Under `## Major Incident`, use numbered lists (e.g., `1. Item`) for chronological or sequential events to clearly outline the incident timeline.
- For non-incident sections, use small paragraphs instead of bullet points to describe details (e.g., setting, context, or summary).
- Use key-value pairs (e.g., `**Key**: Description`) for structured details outside of `## Major Incident`.
- Use blockquotes (e.g., `> Summary`) to emphasize key summaries or conclusions.
- Use horizontal rules (`---`) to separate distinct sections if needed.
- Ensure all Markdown elements are properly formatted for clarity and readability.
"""

def list_transcript_files(bucket, prefix):
    """List all .json files in the given S3 bucket and prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [
            item['Key'] for item in response.get('Contents', [])
            if item['Key'].endswith('.json') and 'chunk_start' in item['Key']
        ]
        logging.info(f"Found {len(files)} transcript files at s3://{bucket}/{prefix}: {files}")
        return files
    except Exception as e:
        logging.error(f"Error listing transcript files in s3://{bucket}/{prefix}: {e}")
        return []

def merge_transcripts(bucket, keys):
    """Merge transcript files into a single video_context string."""
    results = []
    for key in sorted(keys):
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            content = obj['Body'].read().decode('utf-8')
            data = json.loads(content)
            if isinstance(data, list):
                results.extend(data)
            else:
                results.append(data)
        except json.JSONDecodeError as e:
            logging.warning(f"Skipping invalid JSON in {key}: {e}")
        except Exception as e:
            logging.error(f"Error fetching transcript {key}: {e}")

    video_context = ""
    for item in results:
        if isinstance(item, dict):
            for key in sorted(item):
                video_context += f"**************{key}**************\n"
                video_context += f"{item[key]}\n\n"
        else:
            logging.warning(f"Unexpected item type in results: {type(item)} -> {item}")

    return video_context

def normalize_conversation(conversation):
    """Convert conversation to Bedrock-compatible format."""
    normalized = []
    for msg in conversation:
        if isinstance(msg.get('content'), list):
            # Already in Bedrock format
            normalized.append(msg)
        else:
            # Convert {role: string, content: string} to {role: string, content: [{text: string}]}
            normalized.append({
                "role": msg["role"],
                "content": [{"text": msg["content"]}]
            })
    return normalized

def format_to_markdown(response_text):
    """Convert plain text response to Markdown format with headings, paragraphs, and bullet points."""
    sentences = response_text.split('. ')
    sentences = [s.strip() for s in sentences if s.strip()]
    
    markdown = "# Assistant Response\n\n"
    if len(sentences) <= 1:
        markdown += f"{response_text}\n"
    else:
        markdown += f"{sentences[0]}.\n\n"
        if len(sentences) > 1:
            markdown += "## Key Details\n\n"
            for sentence in sentences[1:]:
                markdown += f"- {sentence}.\n"
    
    return markdown

def handler(event, context):
    try:
        logging.info(f"Received event: {json.dumps(event)}")
        
        # Parse request body
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event.get('body', {})

        # Extract required fields
        required_fields = ['videoId', 'executionArn', 's3_dest_uri_w_prefix', 'UserQuery', 'modelId', 'inferenceConfig']
        for field in required_fields:
            if field not in body:
                raise ValueError(f"Missing required field: {field}")

        videoId = body['videoId']
        executionArn = body['executionArn']
        s3_dest_uri_w_prefix = body['s3_dest_uri_w_prefix']

        logging.info(f"Extracted videoId: {videoId}, executionArn: {executionArn}, s3_dest_uri_w_prefix: {s3_dest_uri_w_prefix}")

        # Validate S3 URI format
        expected_prefix = f"s3://cache-us-east-1-054037105643-15bd31e070bd/batch-videos/{videoId}/{executionArn}/chunks/"
        if s3_dest_uri_w_prefix != expected_prefix:
            raise ValueError(f"Invalid S3 URI format. Expected: {expected_prefix}, Got: {s3_dest_uri_w_prefix}")

        # Extract bucket and prefix
        transcript_bucket_name = "cache-us-east-1-054037105643-15bd31e070bd"
        transcript_prefix = s3_dest_uri_w_prefix.replace(f"s3://{transcript_bucket_name}/", "")
        logging.info(f"Checking transcripts in s3://{transcript_bucket_name}/{transcript_prefix}")

        # Check for casual greetings
        user_query = body['UserQuery'].lower().strip()
        greetings = ['hi', 'hello', 'hey', 'greetings']
        is_greeting = any(greeting == user_query for greeting in greetings)

        if is_greeting:
            assistant_response = (
                "# Welcome to Spectra!\n\n"
                "Hello! I'm here to assist you with video analysis. "
                "Ask anything about the video or start a conversation, and I'll provide a detailed response.\n\n"
            )
            message_list = normalize_conversation(body.get('conversation', []))
            message_list.append({"role": "user", "content": [{"text": body['UserQuery']}]})

            chat_response = copy.deepcopy(body)
            message_list.append({"role": "assistant", "content": [{"text": assistant_response}]})
            convo_last_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            chat_response.pop("UserQuery", None)
            chat_response["conversation"] = message_list
            chat_response["chatLastTime"] = convo_last_time
            chat_response["assistantResponse"] = assistant_response
            if not chat_response.get("chatTransactionId"):
                chat_response["chatTransactionId"] = str(uuid.uuid4().hex)

            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
                    'Access-Control-Allow-Credentials': 'true'
                },
                'body': json.dumps(chat_response)
            }

        # List and merge all transcript files
        transcript_keys = list_transcript_files(transcript_bucket_name, transcript_prefix)
        if not transcript_keys:
            error_msg = f"No transcript files found for videoId: {videoId}, executionArn: {executionArn} at {transcript_prefix}. Ensure the executionArn matches the S3 path."
            logging.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg}),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
                    'Access-Control-Allow-Credentials': 'true'
                }
            }

        # Merge transcripts into video_context
        video_context = merge_transcripts(transcript_bucket_name, transcript_keys)
        logging.info(f"Parsed video_context: {video_context}")

        # Prepare system prompt with merged video_context
        system_list = [{"text": system_template.replace("{video_context}", video_context)}]

        # Handle conversation history
        chat_response = copy.deepcopy(body)
        message_list = normalize_conversation(body.get('conversation', []))
        
        # Add Markdown formatting instructions to the user query
        additional_queries = (
            "\n\nPlease provide the response in Markdown format with headers and small paragraphs for clarity. "
            "Place any incidents or major events (e.g., accidents, injuries, robberies, or significant occurrences) under a `## Major Incident` header. "
            "Wrap the entire `## Major Incident` section (including the header and its content) and any incident-related paragraphs (e.g., those describing the incident or its impact) in `<highlight>...</highlight>` tags to indicate they should be highlighted in the frontend. "
            "Under `## Major Incident`, use numbered lists (e.g., `1. Item`) for chronological or sequential events to clearly outline the incident timeline. "
            "For non-incident sections, use small paragraphs instead of bullet points to describe details (e.g., setting, context, or summary). "
            "Use key-value pairs (e.g., `**Key**: Description`) for structured details outside of `## Major Incident`. "
            "Use blockquotes (e.g., `> Summary`) to emphasize key summaries or conclusions. "
            "Use horizontal rules (`---`) to separate distinct sections if needed."
        )
        modified_user_query = body['UserQuery'] + additional_queries
        message_list.append({"role": "user", "content": [{"text": modified_user_query}]})

        # Call Bedrock AI for inference
        logging.info(f"Calling Bedrock with modelId: {body['modelId']}")
        response = client.converse(
            modelId=body["modelId"],
            messages=message_list,
            system=system_list,
            inferenceConfig={
                "temperature": body["inferenceConfig"]["temperature"],
                "topP": body["inferenceConfig"]["topP"],
                "maxTokens": body["inferenceConfig"]["maxTokens"]
            }
        )

        # Extract AI response
        if response and 'output' in response and 'message' in response['output']:
            assistant_response = response['output']['message']['content'][0]['text']
            markdown_response = format_to_markdown(assistant_response)
            logging.info(f"Assistant response (Markdown): {markdown_response}")
        else:
            logging.error("No response from AI model")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'No response from AI model'}),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
                    'Access-Control-Allow-Credentials': 'true'
                }
            }

        # Append AI response to conversation
        message_list.append({"role": "assistant", "content": [{"text": assistant_response}]})

        # Update chat response
        convo_last_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        chat_response.pop("UserQuery", None)
        chat_response["conversation"] = message_list  # Keep Bedrock format
        chat_response["chatLastTime"] = convo_last_time
        chat_response["assistantResponse"] = markdown_response
        if not chat_response.get("chatTransactionId"):
            chat_response["chatTransactionId"] = str(uuid.uuid4().hex)

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
                'Access-Control-Allow-Credentials': 'true'
            },
            'body': json.dumps(chat_response)
        }

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE',
                'Access-Control-Allow-Credentials': 'true'
            }
        }