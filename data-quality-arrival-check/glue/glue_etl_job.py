"""
glue_etl_job.py - AWS Bytes · Data Quality Check · Glue ETL checker (Glue job aws-bytes-dq-glue-etl)

The same job as the Lambda checker (lambda/lambda_checker.py), done with AWS Glue (Spark) and
AWS Glue Data Quality: one landed TLC Yellow month per job run, the same contract (Glue table
yellow_raw), the same statuses, the same outputs and the same file-log row shape.
Approach name everywhere: "glue-etl" (S3 folders, file-log SK glue-etl#compare).

The job reads the ONE S3 file directly (not through the catalog table), so it sees the file's own
column names and types. Spark runs case-sensitive, so "airport_fee" and "Airport_fee" stay two columns.

TIER 1 - FILE checks: "can this whole file be trusted?"   -> quarantine the whole file
  Glue Data Quality decides wherever a DQDL rule exists; own code only where no rule can:
    1 Spark can read the file (Parquet footer readable)          own code (DQ runs only on a readable file)
    2 landed bytes = source bytes (metadata source-bytes)         DQ  FileSize "<file>" = <source-bytes> B
    3 rows > 0                                                    DQ  RowCount > 0
    4 every contract column present                               DQ  ColumnExists "<column>"
    5 names + types = the contract                                DQ  SchemaMatch "contract" = 1.0
    6 spellings = the contract (VendorID, airport_fee, ...)       DQ  ColumnNamesMatchPattern "^(...)$"
    7 the same column twice (by case)                             own code (no DQDL rule)
    8 same file delivered twice (same content, same folder)       DQ  FileUniqueness "<file's folder>" = 1.0
  Notes only, never block (DQ):
    9 new column not in the contract                              DQ  ColumnCount <= <contract columns>
   10 unusual row count vs this feed's history                    DQ  DetectAnomalies "RowCount"
  Rule texts are built at run time from the contract (no hand-kept column list).
  "contract" = an empty DataFrame with the contract's columns and types, passed to Glue DQ as a reference
  dataset. Contract "timestamp" accepts both Spark timestamp kinds (same rule as the Lambda checker).

  All-empty columns: Spark reads a Parquet column that holds no values (e.g. airport_fee in early
  2019-2020 files, written as an empty INT32 column) as "int". A column with no values fits any type
  (same rule as the Lambda checker), so such a column is cast to the contract type before SchemaMatch.
  When a rule fails, own code adds which columns differ to the reason (explanation only; DQ decides).

TIER 2 - ROW checks: "which rows are wrong?"   Runs only when Tier 1 passed. Glue Data Quality,
  row-level results (each row gets DataQualityRulesFail = the rules it failed):
    R1 negative_trip_duration     CustomSql: a row passes when dropoff >= pickup, or a time is NULL
    R2 pickup_outside_file_month  CustomSql: a row passes when pickup is inside the file's month, or NULL
  A NULL time cannot be judged, so that rule does not reject the row (same as the Lambda checker).
  A rejected row keeps every original column plus dq_reasons, e.g. "negative_trip_duration;pickup_outside_file_month".

Results and outputs (zone first, then this approach's folder "glue-etl"):
  passed               -> validated/glue-etl/...   server-side copy of the raw file, byte for byte
  passed_with_rejects  -> validated/glue-etl/...   valid rows only (one new Parquet file)
                          rejected/glue-etl/...    rejected rows + dq_reasons (one new Parquet file)
  quarantined          -> quarantine/glue-etl/...  server-side copy of the raw file (Tier 2 not run)
  A copy left in another zone by an earlier result for the same file is deleted.
  Spark writes a folder of part files; the job writes to _tmp/glue-etl/<the file's own key>/ first (one
  folder per landed file, so parallel runs never share it), then copies the single part file to the final
  key and deletes the _tmp objects.

File log (DynamoDB aws-bytes-dq-file-log): one row per file, SK "glue-etl#compare".
  alert_status = "pending" for quarantined and passed_with_rejects, else "not_needed" (same as Lambda).
  run_id = the Glue job run ID (jr_...) that wrote the row; the measuring run stamps it from Glue's own
  answer (startJobRun.sync), so it is always the real ID. step_ms = time per step (diagnosis).
  dq_rules = every Glue Data Quality rule evaluated for this file with its outcome (Passed / Failed),
  tagged with its ruleset (files / schema / names / notes / rows).

Spark use (one file per job run):
  The landed file is read from S3 ONCE and kept in executor memory (spilled to local disk if it does not
  fit): every ruleset below reads the cached copy, not S3 again. Glue Data Quality's own caching stays off
  (CACHE_NOTHING) because the input is already cached here; caching it twice would only use more memory.
  Tier 2 numbers each row (dq_row_id) and caches that numbered copy before the row rules run, so every
  later step sees the same number for the same row. Each cache is released once nothing needs it.
  read_partitions in the file log = how many Spark partitions (= tasks) the read produced for this file.

Job arguments (set by the measuring run state machine; defaults in infra/template.yaml):
  --file_key            S3 key of the landed file (raw/nyc-taxi/yellow/year=YYYY/month=MM/....parquet)
  --bucket              lake bucket
  --file_log_table      DynamoDB file log
  --glue_database, --glue_table   where the contract lives
  --output_compression  Parquet codec for the rewritten valid / rejected files (e.g. snappy, zstd, gzip)
"""

import datetime as dt                          # timestamps for the log row; month boundaries for rule R2
import re                                      # read the file's year/month from its S3 key
import sys                                     # job arguments
import time                                    # tier1_ms / duration_ms

import boto3                                   # AWS SDK: S3, Glue Data Catalog, DynamoDB
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import StorageLevel

from awsglue.context import GlueContext                # Glue's wrapper around the Spark session
from awsglue.dynamicframe import DynamicFrame          # Glue Data Quality evaluates DynamicFrames
from awsglue.transforms import SelectFromCollection    # picks ruleOutcomes / rowLevelOutcomes
from awsglue.utils import getResolvedOptions           # reads --name value job arguments
from awsgluedq.transforms import EvaluateDataQuality   # Glue Data Quality inside an ETL job

APPROACH = "glue-etl"                          # folder under each zone + file-log SK prefix
SK = f"{APPROACH}#compare"                     # this approach runs as measuring runs only
ZONES = ("validated", "rejected", "quarantine")
PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"
R1, R2 = "negative_trip_duration", "pickup_outside_file_month"
ROW_ID = "dq_row_id"                           # temporary row number: row-level CustomSql returns it for passing rows

s3 = boto3.client("s3")
glue = boto3.client("glue")


# ---------------------------------------------------------------------------
# Spark / Glue setup
# ---------------------------------------------------------------------------
def start_glue():
    """Glue context + Spark settings this checker depends on."""
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    spark.conf.set("spark.sql.caseSensitive", "true")       # keep airport_fee and Airport_fee apart (check 5)
    spark.conf.set("spark.sql.session.timeZone", "UTC")     # month boundaries compared as written, no shift
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # same precision as the source
    return glue_ctx, spark


def job_args():
    """Required job arguments. The run ID is not needed here: the measuring run writes the real Glue
    run ID onto the row after the run (20-Nov: JOB_RUN_ID did not reach the script on Glue 5.0)."""
    return getResolvedOptions(sys.argv, ["file_key", "bucket", "file_log_table", "glue_database", "glue_table",
                                         "output_compression"])


def s3_uri(bucket, key):
    """Where Spark reads and writes (kept in one place so a laptop test can point it elsewhere)."""
    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------------
# The data contract: the Glue table yellow_raw (same reference as the Lambda checker)
# ---------------------------------------------------------------------------
def load_contract(db, table):
    """Columns + types + dq.known_spellings of the Glue table (read on every run, no cache)."""
    t = glue.get_table(DatabaseName=db, Name=table)["Table"]
    spellings = t.get("Parameters", {}).get("dq.known_spellings", "")
    return {
        "types": {c["Name"].lower(): c["Type"].lower() for c in t["StorageDescriptor"]["Columns"]},
        "known_spellings": {x.strip() for x in spellings.split(",") if x.strip()},
    }


# ---------------------------------------------------------------------------
# Glue Data Quality: one helper for both tiers
# ---------------------------------------------------------------------------
def evaluate(glue_ctx, df, ruleset, context, references=None, strict=True):
    """
    Runs a DQDL ruleset on a Spark DataFrame with Glue Data Quality.
    references = {alias: DataFrame} for rules that compare with another dataset (SchemaMatch).
    Returns (rule_outcomes, row_outcomes): rule_outcomes = list of {ruleset, rule, outcome, failure_reason};
    row_outcomes = the DataFrame with the 4 row-level columns added (DataQualityRulesPass / Fail / Skip /
    DataQualityEvaluationResult). Results are published, so every run shows in the Glue console's
    Data quality history (one history per context).
    strict=True: a rule that could not be evaluated stops the job (it must never look like a pass).
    strict=False (note-only rules): it is returned with its own outcome and becomes a note.
    """
    extra = {}
    if references:
        extra["additional_data_sources"] = {alias: DynamicFrame.fromDF(ref, glue_ctx, alias)
                                            for alias, ref in references.items()}
    frames = EvaluateDataQuality().process_rows(
        frame=DynamicFrame.fromDF(df, glue_ctx, context),
        ruleset=ruleset,
        publishing_options={
            "dataQualityEvaluationContext": context,          # name shown in the results history
            "enableDataQualityCloudWatchMetrics": False,      # no CloudWatch metrics (no alarms use them)
            "enableDataQualityResultsPublishing": True,       # keep each run's scores in Glue
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},   # input is already cached by check_file()
        **extra,
    )
    rules = SelectFromCollection.apply(dfc=frames, key="ruleOutcomes").toDF().collect()
    rows = SelectFromCollection.apply(dfc=frames, key="rowLevelOutcomes").toDF()
    outcomes = [{"ruleset": context, "rule": r["Rule"], "outcome": r["Outcome"],
                 "failure_reason": r["FailureReason"] if "FailureReason" in r.asDict() else None}
                for r in rules]
    for o in outcomes:
        if strict and o["outcome"] not in ("Passed", "Failed"):
            raise RuntimeError(f"Glue Data Quality could not evaluate rule {o['rule']}: {o}")
    return outcomes, rows


def failed_rules(outcomes):
    """The rules of one evaluation that failed."""
    return [o for o in outcomes if o["outcome"] == "Failed"]


# ---------------------------------------------------------------------------
# Tier 1
# ---------------------------------------------------------------------------
def head(bucket, key):
    """HeadObject with checksums: size, checksum identity, Lander metadata. Reads no data."""
    h = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    alg = next((k for k in h if k.startswith("Checksum") and k != "ChecksumType"), None)
    identity = f"{alg}:{h.get('ChecksumType', '')}:{h[alg]}" if alg else f"ETag:{h['ETag'].strip(chr(34))}"
    return {"size": h["ContentLength"], "checksum": identity,
            "landed_at": h["LastModified"].isoformat(), "meta": h.get("Metadata", {})}


def root_cause(e):
    """The innermost Java error message (Py4J's own text is only 'An error occurred while calling ...')."""
    je = getattr(e, "java_exception", None)
    if je is None:
        return str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
    while je.getCause() is not None:
        je = je.getCause()
    return f"{je.getClass().getSimpleName()}: {je.getMessage()}"


def dq_str(text):
    """A value inside DQDL double quotes."""
    return text.replace('"', '\\"')


def files_ruleset(bucket, key, source_bytes):
    """Checks 2 + 8: rules on the S3 objects themselves (no rows read by these rules)."""
    folder = key.rsplit("/", 1)[0] + "/"
    rules = [f'FileUniqueness "{dq_str(s3_uri(bucket, folder))}" = 1.0']
    if source_bytes is not None:
        rules.insert(0, f'FileSize "{dq_str(s3_uri(bucket, key))}" = {int(source_bytes)} B')
    return "Rules = [ " + ", ".join(rules) + " ]"


def duplicate_columns(df):
    """Check 7 (own code): the same column twice, compared by case (Spark runs case-sensitive)."""
    seen, dups = {}, []
    for c in df.columns:
        if c.lower() in seen:
            dups.append(f"{seen[c.lower()]} / {c}")
        seen.setdefault(c.lower(), c)
    return dups


def spark_type(hive, file_type):
    """Contract (Hive) type -> Spark type for the reference dataset. 'timestamp' accepts both Spark kinds."""
    if hive == "timestamp" and file_type in (T.TimestampType(), T.TimestampNTZType()):
        return file_type
    return {"bigint": T.LongType(), "int": T.IntegerType(), "smallint": T.ShortType(), "tinyint": T.ByteType(),
            "double": T.DoubleType(), "float": T.FloatType(), "string": T.StringType(),
            "boolean": T.BooleanType(), "timestamp": T.TimestampType()}[hive]


def contract_frames(spark, df, contract):
    """
    The frames the schema rules see:
      checked   = the file's contract columns, lower-case names, all-empty columns cast to the contract type
      named     = the file's contract columns as spelled in the file (for the spelling rule)
      reference = empty DataFrame with the contract's columns and types (SchemaMatch alias "contract")
    New columns are left out of these frames (they are a note, not a reason).
    Also returns which columns differ in type (non-empty columns only), for the reason text.
    """
    file_types = {f.name.lower(): f.dataType for f in df.schema.fields}
    cols = [c for c in df.columns if c.lower() in contract["types"]]
    ref_schema = T.StructType([T.StructField(c, spark_type(t, file_types.get(c)), True)
                               for c, t in contract["types"].items()])
    want = {f.name: f.dataType for f in ref_schema.fields}
    checked, differs = [], []
    for c in cols:
        low, got = c.lower(), file_types[c.lower()]
        if got != want[low]:
            has_value = df.select(F.col(f"`{c}`")).where(F.col(f"`{c}`").isNotNull()).limit(1).count() > 0
            if not has_value:                                 # no values -> fits any type (same as Lambda)
                checked.append(F.col(f"`{c}`").cast(want[low]).alias(low))
                continue
            differs.append(f"{c}: {got.simpleString()} (contract {want[low].simpleString()})")
        checked.append(F.col(f"`{c}`").alias(low))
    return (df.select(checked), df.select([F.col(f"`{c}`") for c in cols]),
            spark.createDataFrame([], ref_schema), differs)


def schema_ruleset(contract):
    """Checks 3, 4, 5."""
    rules = ["RowCount > 0"] + [f'ColumnExists "{c}"' for c in contract["types"]] + ['SchemaMatch "contract" = 1.0']
    return "Rules = [ " + ", ".join(rules) + " ]"


def allowed_spellings(contract):
    """Each contract column may be written lower-case or as one of dq.known_spellings."""
    return sorted(set(contract["types"]) | {s for s in contract["known_spellings"] if s.lower() in contract["types"]})


def names_ruleset(contract):
    """Check 6: every contract column of the file is written in an accepted spelling."""
    return 'Rules = [ ColumnNamesMatchPattern "^(' + "|".join(re.escape(s) for s in allowed_spellings(contract)) + ')$" ]'


def notes_ruleset(contract):
    """Checks 9, 10: notes only."""
    return f'Rules = [ ColumnCount <= {len(contract["types"])}, DetectAnomalies "RowCount" ]'


# ---------------------------------------------------------------------------
# Tier 2
# ---------------------------------------------------------------------------
def file_month(key):
    """The month a file is for, from its key: .../year=YYYY/month=MM/... or ..._YYYY-MM.parquet."""
    m = re.search(r"year=(\d{4})/month=(\d{2})/", key) or re.search(r"_(\d{4})-(\d{2})\.parquet$", key)
    return (int(m.group(1)), int(m.group(2))) if m else None


def tier2_rules(df, ym):
    """
    DQDL for R1 + R2 as row-level CustomSql rules: the query returns the row number of every row that
    PASSES; rows not returned fail that rule. Returns (ruleset, {text that identifies the rule: rule name}).
    """
    r1_sql = (f"select {ROW_ID} from primary where {DROPOFF} >= {PICKUP} "
              f"or {DROPOFF} is null or {PICKUP} is null")
    rules, marks = [f'CustomSql "{r1_sql}"'], {f"{DROPOFF} >= {PICKUP}": R1}
    if ym:
        y, m = ym
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        # literal of the same timestamp kind as the column (Parquet local time = timestamp_ntz)
        kind = "TIMESTAMP_NTZ" if df.schema[PICKUP].dataType.simpleString() == "timestamp_ntz" else "TIMESTAMP"
        start = f"{kind} '{y:04d}-{m:02d}-01 00:00:00'"
        end = f"{kind} '{ny:04d}-{nm:02d}-01 00:00:00'"
        r2_sql = (f"select {ROW_ID} from primary where {PICKUP} is null "
                  f"or ({PICKUP} >= {start} and {PICKUP} < {end})")
        rules.append(f'CustomSql "{r2_sql}"')
        marks[start] = R2
    return "Rules = [ " + ", ".join(rules) + " ]", marks


def rule_name(rule_text, marks):
    """Which of R1/R2 a failed-rule text is; anything else stops the job (never mislabel a row)."""
    hits = [name for mark, name in marks.items() if mark in rule_text]
    if len(hits) != 1:
        raise RuntimeError(f"cannot tell which rule this is: {rule_text}")
    return hits[0]


# ---------------------------------------------------------------------------
# Outputs: S3 zones + file log
# ---------------------------------------------------------------------------
def zone_key(zone, key):
    """raw/nyc-taxi/... -> validated/glue-etl/nyc-taxi/... (zone first, then this approach's folder)."""
    return f"{zone}/{APPROACH}/" + key.split("/", 1)[1]


def spark_write_parquet(df, uri, codec):
    """One Parquet part file (coalesce(1) = one task writes it) into the folder uri, with an explicit codec."""
    df.coalesce(1).write.mode("overwrite").option("compression", codec).parquet(uri)


def write_one_file(df, bucket, staging, final_key, source_key, codec):
    """Spark writes a folder; keep its single part file under final_key, then delete the staging objects."""
    spark_write_parquet(df, s3_uri(bucket, staging), codec)
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=staging).get("Contents", [])
    parts = [o["Key"] for o in objs if o["Key"].rsplit("/", 1)[-1].startswith("part-")]
    if len(parts) != 1:
        raise RuntimeError(f"expected 1 part file under {staging}, found {len(parts)}")
    s3.copy_object(Bucket=bucket, Key=final_key, CopySource={"Bucket": bucket, "Key": parts[0]},
                   Metadata={"source-key": source_key}, MetadataDirective="REPLACE")
    for o in objs:                                            # _tmp is scratch space only
        s3.delete_object(Bucket=bucket, Key=o["Key"])


def publish(bucket, key, res, valid_df, rejected_df, codec):
    """Write this result's outputs, then delete copies an earlier result left in the other zones."""
    written = {}
    if res["status"] in ("passed", "quarantined"):
        zone = "validated" if res["status"] == "passed" else "quarantine"
        s3.copy_object(Bucket=bucket, Key=zone_key(zone, key), CopySource={"Bucket": bucket, "Key": key})
        written[zone] = zone_key(zone, key)
    else:                                                     # passed_with_rejects
        tmp = f"_tmp/{APPROACH}/{key}"                       # one scratch folder per landed file
        write_one_file(valid_df, bucket, f"{tmp}/validated/", zone_key("validated", key), key, codec)
        write_one_file(rejected_df, bucket, f"{tmp}/rejected/", zone_key("rejected", key), key, codec)
        written["validated"] = zone_key("validated", key)
        written["rejected"] = zone_key("rejected", key)
    for zone in ZONES:                                        # re-published file changed result
        if zone not in written:
            s3.delete_object(Bucket=bucket, Key=zone_key(zone, key))   # no error if it does not exist
    return written


def log_row(table, res):
    """Put the result row (measuring run: always written). alert_status as in the Lambda checker.
    run_id is added by the measuring run right after this job run ends."""
    alert_status = "pending" if res["status"] in ("quarantined", "passed_with_rejects") else "not_needed"
    table.put_item(Item=dict(res, checker=SK, alert_status=alert_status,
                             checked_at=dt.datetime.now(dt.timezone.utc).isoformat()))


def mark_copied(table, key, written, publish_ms):
    """Mark the log row done: copied = true + where each output went + how long writing took."""
    table.update_item(Key={"file": key, "checker": SK},
                      UpdateExpression="SET copied = :yes, copied_to = :to, step_ms.publish = :p",
                      ExpressionAttributeValues={":yes": True, ":to": written, ":p": publish_ms})


# ---------------------------------------------------------------------------
# One file, end to end
# ---------------------------------------------------------------------------
def check_file(glue_ctx, spark, args):
    bucket, key = args["bucket"], args["file_key"]
    t0 = time.time()
    steps, clock = {}, [t0]

    def lap(name):                                            # time since the previous step, in ms
        now = time.time()
        steps[name] = int((now - clock[0]) * 1000)
        clock[0] = now
    h = head(bucket, key)
    reasons, notes = [], []
    res = {"file": key, "size_bytes": h["size"], "checksum": h["checksum"], "landed_at": h["landed_at"],
           "source_url": h["meta"].get("source-url"), "reasons": reasons, "notes": notes, "dq_rules": [],
           "step_ms": steps}

    # checks 2 + 8 (Glue DQ, S3 objects only): run first, so a file Spark cannot read still gets them
    src_bytes = h["meta"].get("source-bytes")
    if src_bytes is None:
        notes.append("no source-bytes metadata; FileSize rule skipped")
    rules, _ = evaluate(glue_ctx, spark.range(1).toDF("file_rules"), files_ruleset(bucket, key, src_bytes),
                        "glue-etl-files")
    res["dq_rules"] += rules
    for r in failed_rules(rules):
        detail = f" (landed {h['size']} B)" if r["rule"].startswith("FileSize") else ""
        reasons.append(f"Glue DQ rule failed: {r['rule']}{detail}")
    lap("dq_files")

    # check 1 (own code): Spark can read it. persist() + count() = the ONE read of the file from S3: count()
    # is the first action, so it fills the cache; every later ruleset reads the cached copy, not S3.
    try:
        df = spark.read.parquet(s3_uri(bucket, key)).persist(StorageLevel.MEMORY_AND_DISK)
        res["num_rows"] = df.count()
        res["read_partitions"] = df.rdd.getNumPartitions()   # partitions = tasks per stage on this file
        lap("read")
    except Exception as e:                                    # Spark raises Java errors through Py4J
        reasons.append(f"not readable Parquet: {root_cause(e)[:300]}")
        res.update(status="quarantined", tier1_ms=int((time.time() - t0) * 1000))
        return res, None, None

    # check 7 (own code): the same column twice -> the name-based rules cannot run
    dups = duplicate_columns(df)
    if dups:
        reasons += [f"ambiguous columns {d}" for d in dups]
        notes.append("duplicate column names; Glue DQ schema rules not run")
        res.update(status="quarantined", tier1_ms=int((time.time() - t0) * 1000))
        return res, None, None

    # checks 3-6 (Glue DQ), against the contract
    contract = load_contract(args["glue_database"], args["glue_table"])
    checked, named, reference, differs = contract_frames(spark, df, contract)
    lap("prepare")
    rules, _ = evaluate(glue_ctx, checked, schema_ruleset(contract), "glue-etl-schema",
                        references={"contract": reference})
    res["dq_rules"] += rules
    for r in failed_rules(rules):
        detail = f" ({'; '.join(differs)})" if r["rule"].startswith("SchemaMatch") and differs else ""
        reasons.append(f"Glue DQ rule failed: {r['rule']}{detail}")
    lap("dq_schema")
    rules, _ = evaluate(glue_ctx, named, names_ruleset(contract), "glue-etl-names")
    res["dq_rules"] += rules
    if failed_rules(rules):
        odd = [c for c in named.columns if c not in allowed_spellings(contract)]
        reasons.append(f"Glue DQ rule failed: ColumnNamesMatchPattern (not in the contract: {', '.join(odd)})")
    lap("dq_names")

    # checks 9-10 (Glue DQ, notes only): a rule that cannot run here becomes a note, never a block
    try:
        rules, _ = evaluate(glue_ctx, df.toDF(*[c.lower() for c in df.columns]), notes_ruleset(contract),
                            "glue-etl-notes", strict=False)
        res["dq_rules"] += rules
        for r in rules:
            if r["outcome"] == "Failed" and r["rule"].startswith("ColumnCount"):
                new = [c for c in df.columns if c.lower() not in contract["types"]]
                notes.append(f"new column(s) not in the contract: {', '.join(new)}")
            elif r["outcome"] == "Failed":                     # an anomaly, or the rule could not run (reason says which)
                notes.append(f"Glue DQ note rule failed: {r['rule']}: {r['failure_reason']}")
            elif r["outcome"] != "Passed":
                notes.append(f"Glue DQ note rule not evaluated: {r['rule']} ({r['outcome']})")
    except Exception as e:
        notes.append(f"Glue DQ note rules not run: {root_cause(e)[:200]}")
    lap("dq_notes")

    res["tier1_ms"] = int((time.time() - t0) * 1000)
    if reasons:
        res["status"] = "quarantined"
        return res, None, None

    # ---- Tier 2 (rows) ----
    # monotonically_increasing_id() is built from the partition number + the row's place in it, so it is only
    # stable while the partitions stay the same. Cache the numbered rows and fill the cache now (count()),
    # so the row rules and every later step read the SAME numbers; then the unnumbered copy is not needed.
    low = (df.toDF(*[c.lower() for c in df.columns])
           .withColumn(ROW_ID, F.monotonically_increasing_id())
           .persist(StorageLevel.MEMORY_AND_DISK))
    low.count()
    df.unpersist()
    lap("number_rows")
    ym = file_month(key)
    if not ym:
        notes.append(f"file month not found in the key; rule {R2} skipped")
    ruleset, marks = tier2_rules(low, ym)
    rules, rows = evaluate(glue_ctx, low, ruleset, "glue-etl-rows")
    res["dq_rules"] += rules

    # failed-rule texts -> R1 / R2 flags (one boolean column per rule)
    failed = F.coalesce(F.col("DataQualityRulesFail"), F.array().cast("array<string>"))
    def failed_rule_matches(mark):                            # Spark lambda: one argument only
        return lambda x: x.contains(mark)
    flagged = rows
    for mark, name in marks.items():
        flagged = flagged.withColumn(f"_bad_{name}", F.exists(failed, failed_rule_matches(mark)))
    bad = F.lit(False)
    for name in marks.values():
        bad = bad | F.col(f"_bad_{name}")
    flagged = flagged.withColumn("_bad", bad).persist(StorageLevel.MEMORY_AND_DISK)   # read once, used 3x

    # every failed-rule text must be one of R1 / R2 (no silent mislabel)
    for (text,) in flagged.select(F.explode(failed).alias("t")).distinct().collect():
        rule_name(text, marks)

    agg = flagged.agg(F.count(F.lit(1)).alias("n"),
                      F.sum(F.col("_bad").cast("long")).alias("bad"),
                      *[F.sum(F.col(f"_bad_{n}").cast("long")).alias(n) for n in marks.values()]).first()
    counts = {R1: int(agg[R1] or 0), R2: int(agg[R2] or 0) if R2 in marks.values() else 0}
    n_rejected = int(agg["bad"] or 0)
    res.update(rows_valid=int(agg["n"]) - n_rejected, rows_rejected=n_rejected, rejects_by_rule=counts)
    low.unpersist()                                           # flagged is fully cached now; the writes read it
    lap("dq_rows")

    # back to the file's own column names and order; rejected rows get dq_reasons ("R1;R2", NULLs skipped)
    original = [F.col(c.lower()).alias(c) for c in df.columns]
    valid_df = flagged.filter(~F.col("_bad")).select(original)
    why = F.concat_ws(";", *[F.when(F.col(f"_bad_{n}"), F.lit(n)) for n in marks.values()])
    rejected_df = flagged.filter(F.col("_bad")).select(original + [why.alias("dq_reasons")])
    res["status"] = "passed_with_rejects" if n_rejected else "passed"
    return res, valid_df, rejected_df


def main():
    glue_ctx, spark = start_glue()
    args = job_args()
    if not args["file_key"].startswith("raw/"):               # never check our own outputs
        raise ValueError(f"file_key must be under raw/: {args['file_key']}")
    t0 = time.time()
    res, valid_df, rejected_df = check_file(glue_ctx, spark, args)
    res["duration_ms"] = int((time.time() - t0) * 1000)
    table = boto3.resource("dynamodb").Table(args["file_log_table"])
    log_row(table, dict(res, copied=False))
    t1 = time.time()
    written = publish(args["bucket"], args["file_key"], res, valid_df, rejected_df, args["output_compression"])
    mark_copied(table, args["file_key"], written, int((time.time() - t1) * 1000))
    print({k: res.get(k) for k in ("file", "status", "num_rows", "rows_rejected", "rejects_by_rule", "reasons")})


if __name__ == "__main__":
    main()
