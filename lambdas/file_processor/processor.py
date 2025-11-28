"""
Lambda: File Metadata Processor
-------------------------------

Triggered by S3 ObjectCreated events for keys under the "raw/" prefix.

Pipeline:
    1. Receive event from EventBridge/S3.
    2. Download the raw uploaded file from S3 (/raw/<uuid>-filename>).
    3. Extract basic metadata (size, mime type, sha256 hash).
    4. Write a processed JSON summary to S3 under /processed/<uuid>.json.
    5. Update the corresponding DynamoDB record:
           - Status      → "done"
           - ProcessedKey → processed/<uuid>.json

Environment Variables:
    TABLE_NAME : DynamoDB table storing file metadata.

S3 Structure:
    raw/<uuid>-<filename>
    processed/<uuid>.json

Intended Use:
    This Lambda forms the asynchronous "processing" stage of the pipeline.
    It is lightweight, stateless, and easily replaceable with more advanced
    processing logic (PDF extraction, NLP, etc.).
"""

import json
import os
import mimetypes
import hashlib
import boto3
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    # 1. Extract S3 metadata from EventBridge/S3 event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    raw_key = record["s3"]["object"]["key"]

    # Example key: raw/<uuid>-filename.pdf
    filename = raw_key.split("/")[-1] # Extract <uuid>-filename.pdf
    file_id = filename.split("-")[0]  # Extract uuid

    # 2. Download file to Lambda's ephermeral storage
    local_path = f"/tmp/{filename}"
    s3.download_file(bucket, raw_key, local_path)

    # 3. Processing: extract basic metadata
    size_bytes = os.path.getsize(local_path)
    mime_type, _ = mimetypes.guess_type(filename)

    # 3. Generate sha256, if sha256 hash exists in DynamoDB skip processing to avoid deduplication
    sha256_hash = hashlib.sha256()
    with open(local_path, "rb") as f:
        sha256_hash.update(f.read())
    sha256 = sha256_hash.hexdigest()

    response = dynamodb.query(
        TableName=table_name,
        IndexName="Sha256Index",
        KeyConditionExpression="sha256 = :h",
        ExpressionAttributeValues={
            ":h": {"S": sha256}
        }
    )

    # If a matching sha256 exists, skip processing
    if response["Count"] > 0:
        existing_item = response["Items"][0]
        processed_key = existing_item.get("processedKey", {}).get("S", None)

        dynamodb.update_item(
            TableName=table_name,
            Key={"id": {"S": file_id}},
            UpdateExpression="SET ProcessingState = :state, processedKey = :pk, sha256 = :sha",
            ExpressionAttributeValues={
                ":state": {"S": "done"},
                ":pk": {"S": processed_key},
                ":sha": {"S": sha256}
            }
        )

        return {"statusCode": 200, "body": "DEDUP_OK"}

    processed_output = {
        "file_id": file_id,
        "raw_filename": filename,
        "size_bytes": size_bytes,
        "mime_type": mime_type,
        "sha256": sha256,
        "status": "processed"
    }

    # 4. Upload processed JSON to S3
    processed_key = f"processed/{file_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=json.dumps(processed_output),
        ContentType="application/json"
    )

    # 5. Update DynamoDB
    table_name = os.environ["TABLE_NAME"]
    dynamodb.update_item(
        TableName=table_name,
        Key={"ID": {"S": file_id}},
        UpdateExpression="SET ProcessingStatus = :status, ProcessedKey = :pk, Sha256 = :sha",
        ExpressionAttributeValues={
            ":status": {"S": "done"},
            ":pk": {"S": processed_key},
            ":sha": {"S": sha256}
        }
    )

    return {"statusCode": 200, "body": "OK"}
