"""
footer_checker_lambda.py - AWS Bytes · Data Quality Check · Lambda checker (Lambda aws-bytes-dq-footer-checker)
(The name is kept from the first version, which read only the Parquet footer. The function now runs
 Tier 1 from the footer AND Tier 2 on the rows.)

One landed TLC Yellow month per call. Two tiers of checks:

TIER 1 - FILE checks: "can this whole file be trusted?"
  Reads only S3 metadata + the Parquet footer (a few KB), so its cost does not grow with file size.
    1 readable Parquet footer, row groups add up to num_rows       -> quarantine the whole file
    2 landed bytes = source bytes (incomplete transfer)             -> quarantine
    3 rows > 0                                                      -> quarantine
    4 every contract column present                                 -> quarantine
    5 spelling not in the contract, or the same column twice        -> quarantine
    6 column type differs from the contract (Glue table yellow_raw) -> quarantine
    7 new column not in the contract                                -> pass + note (never blocked)
    8 same file delivered twice (same S3 checksum, live path only)  -> skip
  Recorded, no rule: num_rows, size, null counts, earliest/latest pickup (volume rule on hold).

TIER 2 - ROW checks: "which rows are wrong?"  Runs only when Tier 1 passed.
  Reads the file in batches of rows and checks each row:
    R1 negative trip duration: dropoff earlier than pickup          -> row rejected
    R2 pickup outside the file's month (month taken from the key)   -> row rejected
  A NULL time cannot be judged, so that rule does not reject the row.
  A rejected row keeps every original column plus dq_reasons, e.g. "negative_trip_duration;pickup_outside_file_month".

Results and outputs (zone first, then this path's folder "footer"):
  passed               -> validated/footer/...   server-side copy of the raw file, byte for byte
  passed_with_rejects  -> validated/footer/...   valid rows only (a new Parquet file)
                          rejected/footer/...    rejected rows + dq_reasons
  quarantined          -> quarantine/footer/...  server-side copy of the raw file (Tier 2 not run)
  skipped_duplicate    -> nothing written (live path only)
  A copy left in another zone by an earlier result for the same file is deleted (re-published files).

File log (DynamoDB aws-bytes-dq-file-log): one row per file per mode.
  SK "footer" = live path (S3 event), SK "footer#compare" = measuring run (Step Functions aws-bytes-dq-compare).
  alert_status = "pending" for every quarantined file and every file with rejected rows (live path and
  measuring run), else "not_needed";
  the alert notifier Lambda (alert_notifier_lambda.py) emails pending rows and sets them to "sent".
  run_id = the AWS request ID of the checker Lambda run that wrote the row (his rule, 20-Nov 20:30:
  every row traces back to the execution that created it; CloudWatch START/END lines carry the same ID).
  A duplicate delivery (same checksum, live path) is not processed again: the row keeps its run_id, and
  the duplicate run appends its own request ID to duplicate_run_ids (+ duplicate_count, last_duplicate_at),
  so every execution leaves its trail (his rule, 20-Nov 20:45).

Modes:
  live     EventBridge "Object Created" event   -> conditional log row (duplicate guard) + outputs
  compare  {"bucket","key","mode":"compare"}    -> log row (always written) + outputs; quarantined -> pending
  dry_run  {"bucket","key","mode":"dry_run"}    -> nothing written; Tier 2 counts only (laptop test)

Needs pyarrow: AWS SDK for pandas managed layer in Lambda; `pip install pyarrow boto3` on the laptop.
Env vars (template.yaml): FILE_LOG_TABLE (not needed for dry_run), GLUE_DATABASE, GLUE_TABLE.
"""

import datetime as dt          # timestamps for the log row; month boundaries for rule R2
import os                      # env vars + removing temp files
import re                      # read the file's year/month from its S3 key
import resource                # peak memory of this container (comparison metric)
import struct                  # decode the 4-byte little-endian footer length
import time                    # duration_ms
import urllib.parse            # S3 event keys arrive URL-encoded ("=" -> "%3D")

import boto3                   # AWS SDK: S3, Glue, DynamoDB
import pyarrow as pa           # Parquet footer + row reading
import pyarrow.compute as pc   # vectorised row rules (no Python loop over rows)
import pyarrow.parquet as pq

# Clients are created once per Lambda container and reused across calls
s3 = boto3.client("s3")
glue = boto3.client("glue")

PATH_NAME = "footer"                       # this path's folder under each zone + file-log SK
ZONES = ("validated", "rejected", "quarantine")
PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"
TMP = "/tmp"                               # Lambda ephemeral storage (size set in template.yaml)
BATCH_ROWS = 250_000                       # Tier 2 reads this many rows at a time -> memory stays flat
R1, R2 = "negative_trip_duration", "pickup_outside_file_month"


class NotParquet(Exception):
    """Raised when the file has no valid Parquet footer (check 1)."""


# ---------------------------------------------------------------------------
# The data contract lives in ONE place: the Glue table yellow_raw (infra/template.yaml) =
# the feed's original schema (2019 files), exact types. Same reference for every checker path.
#   - table columns + types              = the expected schema
#   - table parameter dq.known_spellings = original column spellings (Glue stores lower case)
# Read on every call (no cache): a contract change takes effect at once, no Lambda redeploy.
# ---------------------------------------------------------------------------
def load_contract():
    """Read the columns + dq.known_spellings of the Glue table named in the env vars."""
    t = glue.get_table(DatabaseName=os.environ["GLUE_DATABASE"],
                       Name=os.environ["GLUE_TABLE"])["Table"]
    spellings = t.get("Parameters", {}).get("dq.known_spellings", "")
    return {
        # Glue stores lower-case column names and Hive types: bigint / double / timestamp / string
        "types": {c["Name"].lower(): c["Type"].lower() for c in t["StorageDescriptor"]["Columns"]},
        "known_spellings": {x.strip() for x in spellings.split(",") if x.strip()},
    }


# ---------------------------------------------------------------------------
# S3 reads for Tier 1 (metadata + footer only)
# ---------------------------------------------------------------------------
def head(bucket, key):
    """HeadObject with checksums: size, checksum identity, Lander metadata. Reads no data."""
    h = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    # Which checksum S3 stored: ChecksumCRC32 / ChecksumCRC64NVME / ... (depends on the uploader)
    alg = next((k for k in h if k.startswith("Checksum") and k != "ChecksumType"), None)
    if alg:
        # algorithm + type + value: composite CRCs change with part size, so all three must match
        identity = f"{alg}:{h.get('ChecksumType', '')}:{h[alg]}"
    else:
        identity = f"ETag:{h['ETag'].strip(chr(34))}"     # fallback: ETag (multipart ETag != file MD5)
    return {
        "size": h["ContentLength"],
        "checksum": identity,
        "landed_at": h["LastModified"].isoformat(),
        "meta": h.get("Metadata", {}),                      # source-url / source-etag / source-bytes
    }


def ranged(bucket, key, start, end):
    """GET only bytes start..end (inclusive) of the object."""
    return s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")["Body"].read()


def read_footer(bucket, key, size):
    """
    Parquet file = [PAR1][row groups ...][footer][4-byte footer length][PAR1].
    GET 1: last 8 bytes -> footer length + magic.  GET 2: the footer.
    pyarrow parses "PAR1" + footer as if it were the whole file (it only reads metadata).
    Returns (FileMetaData, bytes_read).
    """
    if size < 12:                                           # smaller than header + length + magic
        raise NotParquet(f"file too small ({size} B)")
    tail = ranged(bucket, key, size - 8, size - 1)
    if tail[4:] != b"PAR1":
        raise NotParquet("last 4 bytes are not PAR1 (cut-off or not Parquet)")
    flen = struct.unpack("<I", tail[:4])[0]                 # footer length, little-endian uint32
    if flen + 12 > size:
        raise NotParquet(f"footer length {flen} B larger than the file")
    footer = ranged(bucket, key, size - 8 - flen, size - 1)  # footer + length + magic
    try:
        md = pq.read_metadata(pa.BufferReader(b"PAR1" + footer))
    except Exception as e:                                  # pyarrow raises various types
        raise NotParquet(f"footer does not parse: {e}")
    return md, 8 + len(footer)                              # 2 GETs: 8 B + (footer + 8 B)


# ---------------------------------------------------------------------------
# Tier 1 checks
# ---------------------------------------------------------------------------
def hive_name(arrow_type):
    """Exact Hive/Glue type name of a Parquet column (the contract asks for an exact match)."""
    if pa.types.is_null(arrow_type):
        return "null"           # all-empty column: holds no values, so it fits any type (16-Nov rule)
    for test, name in [(pa.types.is_int64, "bigint"), (pa.types.is_int32, "int"),
                       (pa.types.is_int16, "smallint"), (pa.types.is_int8, "tinyint"),
                       (pa.types.is_float64, "double"), (pa.types.is_float32, "float"),
                       (pa.types.is_timestamp, "timestamp"), (pa.types.is_boolean, "boolean")]:
        if test(arrow_type):
            return name
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "string"
    return str(arrow_type)


def check_schema(md, reasons, notes, c):
    """Checks 4-7 against the contract c (from load_contract).
    reasons = quarantine the file, notes = pass + tell a human."""
    schema = md.schema.to_arrow_schema()
    seen = {}                                               # lower-case name -> original name
    for f in schema:
        low = f.name.lower()
        if low in seen:                                     # check 5: e.g. airport_fee AND Airport_fee in one file
            reasons.append(f"ambiguous columns {seen[low]} / {f.name}")
            continue
        seen[low] = f.name
        if low not in c["types"]:                           # check 7: new column -> pass + note
            notes.append(f"new column {f.name} ({f.type}) not in the contract")
            continue
        if f.name != low and f.name not in c["known_spellings"]:   # check 5: spelling change
            reasons.append(f"column spelling {f.name} not in the contract")
        want, got = c["types"][low], hive_name(f.type)      # check 6: exact type match
        if got != "null" and got != want:                   # all-empty column holds no values -> fits any type
            reasons.append(f"type {f.name}: {got} differs from the contract ({want})")
    for col in c["types"]:                                  # check 4: every contract column present
        if col not in seen:
            reasons.append(f"missing required column {col}")


def column_stats(md):
    """Recorded only: null count per column (summed over row groups) + earliest/latest pickup.
    None = the file wrote no statistics for that column (e.g. 18 months 2023-02 -> 2024-07)."""
    out, pick = {}, None
    for i in range(md.num_columns):
        name = md.schema.column(i).name
        nulls, lo, hi = 0, None, None
        for rg in range(md.num_row_groups):
            st = md.row_group(rg).column(i).statistics
            if st is None or st.null_count is None:
                nulls = None
            elif nulls is not None:
                nulls += st.null_count
            if name.lower() == PICKUP and st is not None and st.has_min_max:
                lo = st.min if lo is None else min(lo, st.min)
                hi = st.max if hi is None else max(hi, st.max)
        out[name.lower()] = nulls
        if name.lower() == PICKUP:
            pick = (lo, hi)
    return out, pick


def tier1(bucket, key):
    """Runs Tier 1 for one object. Returns the result dict (status passed / quarantined), no writes."""
    h = head(bucket, key)
    reasons, notes = [], []
    res = {
        "file": key,
        "size_bytes": h["size"],
        "checksum": h["checksum"],
        "landed_at": h["landed_at"],
        "source_url": h["meta"].get("source-url"),
        "source_etag": h["meta"].get("source-etag"),
        "reasons": reasons,
        "notes": notes,
    }
    # check 2: size vs what the source served (the Lander stamps source-bytes)
    src = h["meta"].get("source-bytes")
    if src is None:
        notes.append("no source-bytes metadata; size check skipped")
    elif int(src) != h["size"]:
        reasons.append(f"size {h['size']} B != source {src} B")
    # check 1: footer readable
    try:
        md, read = read_footer(bucket, key, h["size"])
    except NotParquet as e:
        reasons.append(f"not readable Parquet: {e}")
        res.update(status="quarantined", footer_bytes_read=None)
        return res
    res.update(num_rows=md.num_rows, num_row_groups=md.num_row_groups,
               footer_bytes_read=read, created_by=md.created_by)
    if sum(md.row_group(i).num_rows for i in range(md.num_row_groups)) != md.num_rows:
        reasons.append("row groups do not add up to footer num_rows")      # check 1 (consistency)
    if md.num_rows == 0:                                                     # check 3
        reasons.append("0 rows")
    check_schema(md, reasons, notes, load_contract())                        # checks 4-7
    res["null_counts"], (lo, hi) = column_stats(md)                          # recorded only
    if lo is not None:
        res["pickup_min"], res["pickup_max"] = lo.isoformat(), hi.isoformat()
    res["status"] = "quarantined" if reasons else "passed"
    return res


# ---------------------------------------------------------------------------
# Tier 2 checks (rows)
# ---------------------------------------------------------------------------
def file_month(key):
    """The month a file is for, from its key: .../year=YYYY/month=MM/... or ..._YYYY-MM.parquet."""
    m = re.search(r"year=(\d{4})/month=(\d{2})/", key) or re.search(r"_(\d{4})-(\d{2})\.parquet$", key)
    return (int(m.group(1)), int(m.group(2))) if m else None


def find_col(schema, low_name):
    """Actual spelling of a column in this file (names compared in lower case)."""
    return next((f.name for f in schema if f.name.lower() == low_name), None)


def tier2(bucket, key, res, write):
    """
    Downloads the file to /tmp (one GET) and reads it BATCH_ROWS rows at a time.
    For each batch: R1 + R2 as vectorised masks -> valid rows / rejected rows (+ dq_reasons).
    write=True  -> streams valid rows and rejected rows into two local Parquet files.
    write=False -> counts only (dry run).
    Returns the local paths written (valid, rejected); updates res with the counts.
    """
    src, valid_p, rej_p = f"{TMP}/src.parquet", f"{TMP}/valid.parquet", f"{TMP}/rejected.parquet"
    s3.download_file(bucket, key, src)
    pf = pq.ParquetFile(src)
    schema = pf.schema_arrow
    pick, drop = find_col(schema, PICKUP), find_col(schema, DROPOFF)   # present: Tier 1 checked them

    # R2 bounds: [first day of the file's month, first day of the next month), in the column's own type
    ym = file_month(key)
    if ym:
        y, m = ym
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        ts_type = schema.field(pick).type
        start = pa.scalar(dt.datetime(y, m, 1), type=ts_type)
        end = pa.scalar(dt.datetime(ny, nm, 1), type=ts_type)
    else:
        res["notes"].append("file month not found in the key; rule pickup_outside_file_month skipped")

    r1_txt, r2_txt = pa.scalar(R1), pa.scalar(R2)
    no_txt = pa.scalar(None, type=pa.string())
    valid_w = pq.ParquetWriter(valid_p, schema) if write else None
    rej_w = None                                            # opened only when the first bad row appears
    counts = {R1: 0, R2: 0}
    n_valid = n_rejected = 0
    try:
        for batch in pf.iter_batches(batch_size=BATCH_ROWS):
            t = pa.Table.from_batches([batch], schema=schema)
            p, d = t.column(pick), t.column(drop)
            # fill_null(False): a NULL time cannot be judged, so the rule does not reject the row
            bad1 = pc.fill_null(pc.less(d, p), False)                       # R1 dropoff < pickup
            if ym:
                bad2 = pc.fill_null(pc.or_(pc.less(p, start), pc.greater_equal(p, end)), False)  # R2
            else:
                bad2 = pa.chunked_array([pa.array([False] * t.num_rows)])
            bad = pc.or_(bad1, bad2)
            nb = pc.sum(pc.cast(bad, pa.int64())).as_py() or 0
            counts[R1] += pc.sum(pc.cast(bad1, pa.int64())).as_py() or 0
            counts[R2] += pc.sum(pc.cast(bad2, pa.int64())).as_py() or 0
            n_rejected += nb
            n_valid += t.num_rows - nb
            if not write:
                continue
            valid_w.write_table(t.filter(pc.invert(bad)))
            if nb:
                rows = t.filter(bad)
                # dq_reasons: every rule the row failed, joined with ";"
                why = pc.binary_join_element_wise(
                    pc.if_else(pc.filter(bad1, bad), r1_txt, no_txt),
                    pc.if_else(pc.filter(bad2, bad), r2_txt, no_txt),
                    ";", null_handling="skip")
                rows = rows.append_column("dq_reasons", why)
                if rej_w is None:
                    rej_w = pq.ParquetWriter(rej_p, rows.schema)
                rej_w.write_table(rows)
    finally:
        if valid_w:
            valid_w.close()
        if rej_w:
            rej_w.close()
        os.remove(src)                                      # the container is reused: free /tmp now

    res.update(rows_valid=n_valid, rows_rejected=n_rejected, rejects_by_rule=counts,
               bytes_read_tier2=res["size_bytes"])
    if n_rejected:
        res["status"] = "passed_with_rejects"
    return (valid_p if write else None), (rej_p if rej_w else None)


def check_file(bucket, key, write, gate=None):
    """Tier 1, then Tier 2 only if Tier 1 passed. Returns (result, valid_path, rejected_path).
    gate(res) is called right after Tier 1 (live path: the duplicate guard); if it returns False
    the call stops there, so a duplicate never pays for reading its rows. Result is then None."""
    t0 = time.time()
    res = tier1(bucket, key)
    res["tier1_ms"] = int((time.time() - t0) * 1000)
    if gate and not gate(res):
        return None, None, None
    valid_p = rej_p = None
    if res["status"] == "passed":
        valid_p, rej_p = tier2(bucket, key, res, write)
    res["duration_ms"] = int((time.time() - t0) * 1000)
    # peak memory of this container so far (MB; Linux reports KB) - Lambda sizing evidence
    res["max_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    return res, valid_p, rej_p


# ---------------------------------------------------------------------------
# Writes: zone outputs (S3) + file log (DynamoDB)
# ---------------------------------------------------------------------------
def zone_key(zone, key):
    """raw/nyc-taxi/... -> validated/footer/nyc-taxi/... (zone first, then this checker's folder)."""
    return f"{zone}/{PATH_NAME}/" + key.split("/", 1)[1]


def publish(bucket, key, res, valid_p, rej_p):
    """Write this result's outputs, then delete copies an earlier result left in the other zones.
    Returns {zone: key written}."""
    written = {}
    try:
        if res["status"] in ("passed", "quarantined"):
            zone = "validated" if res["status"] == "passed" else "quarantine"
            s3.copy_object(Bucket=bucket, Key=zone_key(zone, key),       # server-side copy, keeps metadata
                           CopySource={"Bucket": bucket, "Key": key})
            written[zone] = zone_key(zone, key)
        else:                                                            # passed_with_rejects
            meta = {"Metadata": {"source-key": key}}
            s3.upload_file(valid_p, bucket, zone_key("validated", key), ExtraArgs=meta)
            s3.upload_file(rej_p, bucket, zone_key("rejected", key), ExtraArgs=meta)
            written["validated"] = zone_key("validated", key)
            written["rejected"] = zone_key("rejected", key)
        for zone in ZONES:                                               # re-published file changed result
            if zone not in written:
                s3.delete_object(Bucket=bucket, Key=zone_key(zone, key))  # no error if it does not exist
    finally:
        for p in (valid_p, rej_p):                                       # free /tmp for the next call
            if p and os.path.exists(p):
                os.remove(p)
    return written


def log_row(table, res, sk, conditional, run_id):
    """
    Put the result row. Live mode is conditional (check 8, duplicate):
    write only if no row yet, OR the checksum changed (re-publish), OR the earlier run never copied
    (copied = false lets a failed run be retried; two simultaneous deliveries can both pass - harmless,
    the outputs are the same). Returns False when the condition blocks it = same file already processed.
    """
    # alert_status = is this file still owed a notification? (his rules, 20-Nov 18:40 + 20:00)
    #   "pending"    -> quarantined, or passed with rejected rows (live path AND measuring run)
    #   "not_needed" -> passed clean
    #   "sent"       -> written by the alert notifier after it emailed this row
    alert_status = "pending" if res["status"] in ("quarantined", "passed_with_rejects") else "not_needed"
    item = dict(res, checker=sk, checked_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                alert_status=alert_status,
                run_id=run_id)                 # which checker execution wrote this row (Lambda request ID)
    kwargs = {"Item": item}
    if conditional:
        kwargs["ConditionExpression"] = "attribute_not_exists(#f) OR checksum <> :c OR copied = :no"
        kwargs["ExpressionAttributeNames"] = {"#f": "file"}
        kwargs["ExpressionAttributeValues"] = {":c": res["checksum"], ":no": False}
    try:
        table.put_item(**kwargs)
        return True
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def mark_copied(table, key, sk, written):
    """Mark the log row done: copied = true + where each output went."""
    table.update_item(Key={"file": key, "checker": sk},
                      UpdateExpression="SET copied = :yes, copied_to = :to",
                      ExpressionAttributeValues={":yes": True, ":to": written})


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------
def handler(event, context=None):
    if "detail" in event:                                   # EventBridge S3 "Object Created"
        bucket = event["detail"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(event["detail"]["object"]["key"])
        mode = "live"
    else:                                                   # Step Functions / laptop
        bucket, key, mode = event["bucket"], event["key"], event.get("mode", "dry_run")
    if not key.startswith("raw/"):                          # loop guard: never check our own outputs
        return {"file": key, "status": "ignored", "reason": "not under raw/"}

    if mode == "dry_run":                                   # laptop: counts only, nothing written
        return check_file(bucket, key, write=False)[0]

    table = boto3.resource("dynamodb").Table(os.environ["FILE_LOG_TABLE"])
    run_id = getattr(context, "aws_request_id", "local")   # this execution's ID, stamped on the log row
    if mode == "compare":
        # measuring run: always write the row (no duplicate guard), same outputs as the live path;
        # a re-run resets quarantined / passed_with_rejects files to "pending", so they are reported again (his rule)
        sk = f"{PATH_NAME}#compare"
        res, valid_p, rej_p = check_file(bucket, key, write=True)
        log_row(table, dict(res, copied=False), sk, conditional=False, run_id=run_id)
        res["copied_to"] = publish(bucket, key, res, valid_p, rej_p)
        mark_copied(table, key, sk, res["copied_to"])
        return dict(res, run_id=run_id)

    # live: Tier 1 -> conditional log row (copied = false) = the lock / duplicate guard (check 8)
    #       -> Tier 2 -> final log row -> outputs -> mark copied
    sk = PATH_NAME

    def claim(t1_res):
        """Duplicate guard right after Tier 1: False = same file already processed."""
        if log_row(table, dict(t1_res, copied=False), sk, conditional=True, run_id=run_id):
            return True
        table.update_item(Key={"file": key, "checker": sk},                 # count the skip
                          UpdateExpression="ADD duplicate_count :one SET last_duplicate_at = :t, "
                                           "duplicate_run_ids = list_append(if_not_exists(duplicate_run_ids, :none), :r)",
                          ExpressionAttributeValues={":one": 1,
                                                     ":t": dt.datetime.now(dt.timezone.utc).isoformat(),
                                                     ":none": [], ":r": [run_id]})   # every duplicate run's ID
        return False

    res, valid_p, rej_p = check_file(bucket, key, write=True, gate=claim)
    if res is None:
        return {"file": key, "status": "skipped_duplicate", "run_id": run_id}
    log_row(table, dict(res, copied=False), sk, conditional=False, run_id=run_id)   # this call owns the row now
    res["copied_to"] = publish(bucket, key, res, valid_p, rej_p)
    mark_copied(table, key, sk, res["copied_to"])
    return dict(res, run_id=run_id)


if __name__ == "__main__":
    # Laptop dry run as the runner role:  python footer_checker_lambda.py BUCKET KEY
    # (set GLUE_DATABASE=aws_bytes_lake and GLUE_TABLE=yellow_raw first: the contract is read from Glue;
    #  set TMP_DIR if /tmp does not exist, e.g. on Windows)
    import json
    import sys
    TMP = os.environ.get("TMP_DIR", TMP)
    print(json.dumps(handler({"bucket": sys.argv[1], "key": sys.argv[2], "mode": "dry_run"}),
                     indent=2, default=str))
