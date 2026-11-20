"""
lander_lambda.py - AWS Bytes · Data Quality Check · Lander (Lambda aws-bytes-dq-lander)

Lands ONE NYC TLC Yellow month from TLC's CloudFront site into the lake's raw/ zone.
Called by the Step Functions land run (aws-bytes-dq-land), one call per month.

Input : {"month": "2023-01"}
Output: {"month", "key", "status", "bytes", ...}
  status = landed             first copy of this month
         | relanded           TLC re-published the file (ETag or size changed) -> overwritten
         | skipped_unchanged  same source ETag + size already in raw/ (no download, no S3 event)
         | not_available      CloudFront still 403/404 after 3 tries (month not published yet)
         | size_mismatch      landed size != source size (incomplete copy)

Steps:
  1. HEAD the CloudFront URL (retry 403/429/5xx after 0 / 5 / 15 s - CloudFront once returned
     403 for a real month and served it minutes later)
  2. HEAD our raw/ copy; same source ETag + size -> skip (re-landing = new S3 event = re-check)
  3. Stream CloudFront -> S3 multipart upload (never held whole in memory or /tmp),
     stamping source-url / source-etag / source-bytes as object metadata (the checker uses them)
  4. HEAD again: landed size must equal source size
  5. Register the Glue partition year=/month= (AlreadyExists = fine)

Env vars: LAKE_BUCKET, GLUE_DATABASE, GLUE_TABLE (set by template.yaml)
"""

import os                       # read the env vars above
import time                     # sleep between retries
import urllib.error             # HTTPError carries CloudFront's status code
import urllib.request           # plain HTTP HEAD / GET to CloudFront (no extra library)

import boto3                    # AWS SDK: S3 + Glue
from botocore.exceptions import ClientError

# TLC publishes one Parquet file per month at this public CloudFront address
SOURCE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
RETRY_CODES = {403, 429, 500, 502, 503, 504}   # worth retrying; any other code fails fast
WAITS = [0, 5, 15]                              # seconds to wait before attempt 1, 2, 3

# Clients are created once per Lambda container and reused across calls
s3 = boto3.client("s3")
glue = boto3.client("glue")


def open_source(url, method):
    """HEAD or GET the CloudFront URL with retries.
    Returns the open HTTP response, or None if the month is not published (403/404)."""
    last = None
    for wait in WAITS:
        time.sleep(wait)
        try:
            # urlopen returns a stream: for GET the body is read later, piece by piece
            return urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=60)
        except urllib.error.HTTPError as e:
            last = e.code
            if e.code not in RETRY_CODES:
                break                            # e.g. 404: retrying will not help
    if last in (403, 404):
        return None                              # TLC answers 403 for a month not yet published
    raise RuntimeError(f"{method} {url} failed with HTTP {last} after {len(WAITS)} tries")


def raw_head(bucket, key):
    """HeadObject of our copy; None if it doesn't exist.
    (S3 returns 404 for a missing key only if the caller has s3:ListBucket, else 403.)"""
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise                                    # anything else is a real error


def add_partition(bucket, db, table, year, month):
    """Register year=/month= in the Glue table, copying the table's storage settings
    (columns, Parquet SerDe) and pointing the partition at this month's folder."""
    sd = dict(glue.get_table(DatabaseName=db, Name=table)["Table"]["StorageDescriptor"])
    sd["Location"] = f"s3://{bucket}/raw/nyc-taxi/yellow/year={year}/month={month}/"
    try:
        glue.create_partition(DatabaseName=db, TableName=table,
                              PartitionInput={"Values": [year, month], "StorageDescriptor": sd})
        return "created"
    except glue.exceptions.AlreadyExistsException:
        return "exists"                          # re-runs are safe


def handler(event, context=None):
    bucket, db, table = os.environ["LAKE_BUCKET"], os.environ["GLUE_DATABASE"], os.environ["GLUE_TABLE"]
    month = event["month"]                                   # "YYYY-MM"
    year, mm = month.split("-")
    url = SOURCE.format(m=month)
    # Hive-style folders (year=/month=) so Glue/Athena can prune by partition
    key = f"raw/nyc-taxi/yellow/year={year}/month={mm}/yellow_tripdata_{month}.parquet"
    out = {"month": month, "key": key}

    # 1. what does the source have right now? (size + ETag, no download)
    head = open_source(url, "HEAD")
    if head is None:
        return dict(out, status="not_available", bytes=0)
    src_bytes = int(head.headers["Content-Length"])
    src_etag = head.headers.get("ETag", "").strip('"')

    # 2. same file already landed? -> skip (no download, no new S3 event, no re-check)
    mine = raw_head(bucket, key)
    if mine and mine["Metadata"].get("source-etag") == src_etag and mine["ContentLength"] == src_bytes:
        return dict(out, status="skipped_unchanged", bytes=0,
                    partition=add_partition(bucket, db, table, year, mm))

    # 3. stream CloudFront -> S3 (boto3 splits it into 8 MiB multipart parts)
    body = open_source(url, "GET")
    if body is None:
        return dict(out, status="not_available", bytes=0)
    s3.upload_fileobj(body, bucket, key, ExtraArgs={"Metadata": {
        "source-url": url, "source-etag": src_etag, "source-bytes": str(src_bytes)}})

    # 4. verify the landed size against what the source announced
    landed = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if landed != src_bytes:
        return dict(out, status="size_mismatch", bytes=landed, source_bytes=src_bytes)

    # 5. make the month queryable in Glue/Athena
    return dict(out, status="relanded" if mine else "landed", bytes=landed,
                partition=add_partition(bucket, db, table, year, mm))
