"""
alert_notifier_lambda.py - AWS Bytes · Data Quality Check · alert notifier (Lambda aws-bytes-dq-alert-notifier)

Turns the file log's to-do list into ONE email:
  1. Reads every file-log row with alert_status = "pending" (= a quarantined file, or a file with rejected
     rows; any path, any run)
  2. Sends ONE short email through SNS: its Alert ID, the check period it covers, files quarantined +
     invalid trip records since the last notification, and the console query that lists exactly
     those rows (the full reasons per file stay in the file log; the email is a notification, not a report)
  3. Marks each emailed row alert_status = "sent" + alerted_at + alert_id + alert_run_id, so the next
     run does not repeat it and every email traces back to its rows (his rule, 20-Nov 20:30)
  No pending rows -> no email.

Trace IDs on a file-log row:
  run_id        = request ID of the checker run that wrote the row (set by the checker)
  alert_id      = DQ-ALERT-<UTC yyyymmdd-hhmmss>Z, the ID printed in the email that reported the row
  alert_run_id  = request ID of the notifier run that sent that email (matches its CloudWatch logs)

Triggered by EventBridge Scheduler every 15 minutes (template.yaml), or by hand:
  aws lambda invoke --function-name aws-bytes-dq-alert-notifier ... (runbook section 6b)

Delivery is at-least-once: the email is sent BEFORE the rows are marked, so if marking fails the same
files are reported again next time (never silently lost); the row then carries the later email's alert_id. Two notifier runs at the same moment could
send the same list twice; each row is still marked only once (conditional update).

Env vars (set by template.yaml): FILE_LOG_TABLE, TOPIC_ARN
"""

import datetime as dt            # alerted_at timestamp
import os                        # env vars

import boto3                     # AWS SDK: DynamoDB + SNS
from boto3.dynamodb.conditions import Attr   # scan filter: alert_status = pending

sns = boto3.client("sns")


def pending_rows(table):
    """Every row with alert_status = pending (Scan pages through the whole table)."""
    rows, kwargs = [], {"FilterExpression": Attr("alert_status").eq("pending")}
    while True:
        page = table.scan(**kwargs)
        rows += page["Items"]
        if "LastEvaluatedKey" not in page:
            return rows
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def short_time(iso):
    """'2026-11-20T14:45:03.123+00:00' -> '2026-11-20 14:45 UTC' (readable in an email)."""
    return iso[:16].replace("T", " ") + " UTC" if iso else "unknown"


def build_message(rows, alert_id):
    """
    The short notification (his wording, 20-Nov 19:59) + the Alert ID and the period it covers
    (20-Nov 20:30), so each email maps to exactly its rows. Nothing per file: details live in the file log.
    """
    table = os.environ["FILE_LOG_TABLE"]
    files = sum(1 for r in rows if r.get("status") == "quarantined")
    records = sum(int(r.get("rows_rejected", 0)) for r in rows if r.get("status") == "passed_with_rejects")
    checked = sorted(r["checked_at"] for r in rows if r.get("checked_at"))
    period = f"{short_time(checked[0])} to {short_time(checked[-1])}" if checked else "unknown"
    subject = f"{alert_id}: {files:,} file(s) quarantined, {records:,} invalid trip record(s)"
    body = "\n".join([
        f"Alert ID: {alert_id}",
        f"Covers checks from {period}.",
        "",
        f"{files:,} file(s) quarantined since the last notification.",
        f"{records:,} trip record(s) identified as invalid since the last notification.",
        "",
        f"Details: DQ tracking table {table}, rows with alert_id = {alert_id}",
        "(DynamoDB console > PartiQL editor):",
        f"SELECT * FROM \"{table}\" WHERE alert_id = '{alert_id}'",
    ])
    return subject, body


def handler(event=None, context=None):
    table = boto3.resource("dynamodb").Table(os.environ["FILE_LOG_TABLE"])
    rows = pending_rows(table)
    if not rows:
        return {"pending": 0, "emailed": 0}                 # nothing to report -> no email

    sent_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    now = sent_at.isoformat()                                                # one timestamp for every marked row
    alert_id = "DQ-ALERT-" + sent_at.strftime("%Y%m%d-%H%M%S") + "Z"         # the ID printed in the email
    alert_run_id = getattr(context, "aws_request_id", "local")              # this notifier execution
    subject, body = build_message(rows, alert_id)
    sns.publish(TopicArn=os.environ["TOPIC_ARN"], Subject=subject, Message=body)

    marked = 0
    for r in rows:                                          # mark each row; skip one another run already marked
        try:
            table.update_item(Key={"file": r["file"], "checker": r["checker"]},
                              UpdateExpression="SET alert_status = :sent, alerted_at = :t, "
                                               "alert_id = :a, alert_run_id = :r",
                              ConditionExpression="alert_status = :pending",
                              ExpressionAttributeValues={":sent": "sent", ":t": now, ":pending": "pending",
                                                         ":a": alert_id, ":r": alert_run_id})
            marked += 1
        except table.meta.client.exceptions.ConditionalCheckFailedException:
            pass
    return {"pending": len(rows), "emailed": len(rows), "marked_sent": marked, "subject": subject,
            "alert_id": alert_id, "alert_run_id": alert_run_id}
