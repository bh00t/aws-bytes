"""
alert_notifier_lambda.py - AWS Bytes · Data Quality Check · alert notifier (Lambda aws-bytes-dq-alert-notifier)

Turns the file log's to-do list into ONE email PER APPROACH (lambda, glue-etl):
  1. Reads every file-log row with alert_status = "pending" (= a quarantined file, or a file with rejected
     rows; live path or measuring run)
  2. Groups the rows by approach, taken from the row's checker key (SK):
       lambda / lambda#compare (and the old name footer / footer#compare) -> lambda
       glue-etl#compare                                                  -> glue-etl
  3. For each approach with pending rows, sends ONE short email through SNS: the approach, its own Alert ID,
     the check period it covers, files quarantined + invalid trip records since the last notification, and
     the console query that lists exactly those rows (the full reasons per file stay in the file log;
     the email is a notification, not a report)
  4. Marks each emailed row alert_status = "sent" + alerted_at + alert_id + alert_run_id, so the next
     run does not repeat it and every email traces back to its rows (his rule, 20-Nov 20:30)
  No pending rows -> no email.

Trace IDs on a file-log row:
  run_id        = ID of the checker run that wrote the row (Lambda request ID / Glue job run ID)
  alert_id      = DQ-ALERT-<UTC yyyymmdd-hhmmss>Z-<approach>, the ID printed in the email that reported the row
  alert_run_id  = request ID of the notifier run that sent that email (matches its CloudWatch logs)

Triggered by EventBridge Scheduler every 15 minutes (template.yaml), or by hand:
  aws lambda invoke --function-name aws-bytes-dq-alert-notifier ... (runbook section 10)

Delivery is at-least-once: each email is sent BEFORE its rows are marked, so if marking fails the same
files are reported again next time (never silently lost); the row then carries the later email's alert_id.
Two notifier runs at the same moment could send the same list twice; each row is still marked only once
(conditional update).

Env vars (set by template.yaml): FILE_LOG_TABLE, TOPIC_ARN
"""

import datetime as dt            # alerted_at timestamp
import os                        # env vars

import boto3                     # AWS SDK: DynamoDB + SNS
from boto3.dynamodb.conditions import Attr   # scan filter: alert_status = pending

sns = boto3.client("sns")

# checker key (SK) before "#" -> approach name shown in the email ("footer" = the lambda approach's old name)
APPROACH_OF = {"lambda": "lambda", "footer": "lambda", "glue-etl": "glue-etl"}


def pending_rows(table):
    """Every row with alert_status = pending (Scan pages through the whole table)."""
    rows, kwargs = [], {"FilterExpression": Attr("alert_status").eq("pending")}
    while True:
        page = table.scan(**kwargs)
        rows += page["Items"]
        if "LastEvaluatedKey" not in page:
            return rows
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def approach(row):
    """Approach that wrote the row, from its checker key: 'glue-etl#compare' -> 'glue-etl'."""
    prefix = row["checker"].split("#", 1)[0]
    return APPROACH_OF.get(prefix, prefix)                 # an unknown checker still gets its own email


def short_time(iso):
    """'2026-11-20T14:45:03.123+00:00' -> '2026-11-20 14:45 UTC' (readable in an email)."""
    return iso[:16].replace("T", " ") + " UTC" if iso else "unknown"


def build_message(rows, alert_id, name):
    """
    The short notification (his wording, 20-Nov 19:59) + the approach, the Alert ID and the period it covers
    (20-Nov 20:30), so each email maps to exactly its rows. Nothing per file: details live in the file log.
    """
    table = os.environ["FILE_LOG_TABLE"]
    files = sum(1 for r in rows if r.get("status") == "quarantined")
    records = sum(int(r.get("rows_rejected", 0)) for r in rows if r.get("status") == "passed_with_rejects")
    checked = sorted(r["checked_at"] for r in rows if r.get("checked_at"))
    period = f"{short_time(checked[0])} to {short_time(checked[-1])}" if checked else "unknown"
    # SNS allows at most 100 characters in a subject; the approach is already inside the Alert ID
    subject = f"{alert_id}: {files:,} file(s) quarantined, {records:,} invalid trip record(s)"[:100]
    body = "\n".join([
        f"Approach: {name}",
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
        return {"pending": 0, "emails": []}                 # nothing to report -> no email

    sent_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    now = sent_at.isoformat()                                                # one timestamp for every marked row
    alert_run_id = getattr(context, "aws_request_id", "local")              # this notifier execution
    groups = {}
    for r in rows:
        groups.setdefault(approach(r), []).append(r)

    emails = []
    for name in sorted(groups):                                             # one email per approach
        group = groups[name]
        alert_id = "DQ-ALERT-" + sent_at.strftime("%Y%m%d-%H%M%S") + "Z-" + name   # unique per approach
        subject, body = build_message(group, alert_id, name)
        sns.publish(TopicArn=os.environ["TOPIC_ARN"], Subject=subject, Message=body)

        marked = 0
        for r in group:                                     # mark each row; skip one another run already marked
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
        emails.append({"approach": name, "alert_id": alert_id, "rows": len(group),
                       "marked_sent": marked, "subject": subject})
    return {"pending": len(rows), "emails": emails, "alert_run_id": alert_run_id}
