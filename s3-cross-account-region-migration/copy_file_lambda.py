"""copy_file_lambda.py (state: CopyOne) - copies ONE file from the source bucket to the destination bucket
(server-side, cross-region) and checks the destination size.
Called once per file by the Step Functions Map state.

Input event : {"key": "202604-citibike-tripdata.zip", "size": 164592563}
Output      : {"key": ..., "src_size": ..., "dst_size": ..., "ok": true/false}
A size mismatch returns ok=false (no exception); real errors raise, so the
state machine's Retry/Catch handles them.
"""
import json
import os

import boto3
from botocore.config import Config

# A single CopyObject on a 1 GB file can take minutes; the default 60 s read
# timeout is too short. Retries are left to Step Functions (Retry 3x).
s3 = boto3.client(
    "s3",
    config=Config(read_timeout=840, connect_timeout=10, retries={"max_attempts": 1}),
)

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]  # tripdata
DEST_BUCKET = os.environ["DEST_BUCKET"]      # aws-bytes-s3-migration-<account>-ap-south-1
DEST_PREFIX = os.environ["DEST_PREFIX"]      # raw/citibike/stepfn/


def lambda_handler(event, context):
    key = event["key"]
    src_size = event["size"]
    dest_key = DEST_PREFIX + key

    # 1. Server-side copy: bytes move S3 -> S3, never through this Lambda
    s3.copy_object(
        CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
        Bucket=DEST_BUCKET,
        Key=dest_key,
    )

    # 2. Read the destination object's size (HeadObject = metadata only)
    dst_size = s3.head_object(Bucket=DEST_BUCKET, Key=dest_key)["ContentLength"]

    # 3. Compare
    ok = dst_size == src_size

    result = {"key": key, "src_size": src_size, "dst_size": dst_size, "ok": ok}
    print(json.dumps(result))  # one line per file in CloudWatch Logs
    return result
