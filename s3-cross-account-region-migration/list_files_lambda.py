"""ListSource Lambda (approach 2 - Step Functions).

Lists the source files to copy: every key in SOURCE_BUCKET that starts with
SOURCE_PREFIX and ends with SOURCE_SUFFIX. Returns {"files": [{"key", "size"}]}
for the Step Functions Map state. Same code is inline in infra/template.yaml.
"""
import json
import os

import boto3

SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
SOURCE_REGION = os.environ["SOURCE_REGION"]
SOURCE_PREFIX = os.environ["SOURCE_PREFIX"]
SOURCE_SUFFIX = os.environ["SOURCE_SUFFIX"]

s3 = boto3.client("s3", region_name=SOURCE_REGION)


def lambda_handler(event, context):
    files = []
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX)
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(SOURCE_SUFFIX):
                files.append({"key": obj["Key"], "size": obj["Size"]})

    print(json.dumps({"count": len(files), "bytes": sum(f["size"] for f in files)}))
    return {"files": files}
