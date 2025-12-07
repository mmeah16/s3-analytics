print("INVOKING LAMBDA")
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
import re
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_json(message, **fields):
    logger.info(json.dumps({"message": message, **fields}))

s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

def lambda_handler(event, context):
    start = time.Time()

    # 1. Extract S3 metadata from EventBridge/S3 event
    record = event["detail"]
    bucket = record["bucket"]["name"]
    raw_key = record["object"]["key"]

    head = s3.head_object(Bucket=bucket, Key=raw_key)
    print(head)
    metadata = head.get("Metadata", {})

    trace_id = metadata.get("trace_id")

    def log(msg, **fields):
        log_json(msg, component="processor", trace_id=trace_id, key=raw_key, bucket=bucket, **fields)

    log("event_received", event=event)
    # Example key: raw/<uuid>-filename.pdf
    filename = raw_key.split("/")[-1] # Extract <uuid>-filename.pdf
    match = re.match(r"^([0-9a-fA-F-]{36})-", filename)
    file_id = match.group(1) if match else None
    log("file_id_parsed", file_id=file_id)
    # 2. Download file to Lambda's ephermeral storage
    local_path = f"/tmp/{filename}"
    s3.download_file(bucket, raw_key, local_path)
    log("file_downloaded", local_path=local_path)

    # 3. Processing: extract basic metadata
    size_bytes = os.path.getsize(local_path)
    mime_type, _ = mimetypes.guess_type(filename)

    # 3. Generate sha256, if sha256 hash exists in DynamoDB skip processing to avoid deduplication
    sha256_hash = hashlib.sha256()
    with open(local_path, "rb") as f:
        sha256_hash.update(f.read())
    sha256 = sha256_hash.hexdigest()
    log("computed_sha256", sha256=sha256)
    table_name = os.environ["TABLE_NAME"]
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
        log("dedupe_hit", processed_key=processed_key)
        dynamodb.update_item(
            TableName=table_name,
            Key={"id": {"S": file_id}},
            UpdateExpression="SET processingState = :state, processedKey = :pk, sha256 = :sha",
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
    log("uploaded_processed_file", processed_key=processed_key)
    # 5. Update DynamoDB
    dynamodb.update_item(
        TableName=table_name,
        Key={"id": {"S": file_id}},
        UpdateExpression="SET processingState = :status, processedKey = :pk, sha256 = :sha",
        ExpressionAttributeValues={
            ":status": {"S": "done"},
            ":pk": {"S": processed_key},
            ":sha": {"S": sha256}
        }
    )
    log("dynamodb_update_complete")
    log("lambda_completed", latency_ms = int((time.Time()-start)*1000))
    return {"statusCode": 200, "body": "OK"}
