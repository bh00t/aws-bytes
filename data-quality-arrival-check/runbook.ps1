# =============================================================================
# runbook.ps1 - AWS Bytes · Data Quality Check: two approaches (lambda, glue-etl) on one stack
#
# Run ONE line at a time in PowerShell (VS Code terminal). Each section says who runs it.
# Identities:
#   default profile  = IAM user poc-cli (admin) -> deploys and deletes
#   profile dqrunner = IAM role poc-dq-runner (created by the stack) -> starts runs, reads results
# Region ap-south-1. The account ID is read from your credentials (section 0), not typed in.
#
# NOT TOUCHED by anything above section 11: raw/nyc-taxi/yellow/ (the landed files), the Lander, the Glue table
# and its partitions. Sections 11-14 are for the final run and the end of the PoC only.
# Costs: Lambda runs = cents; Glue job runs = 2 DPU each, billed per second with a 1-minute minimum
# (estimates are given before each Glue run).
# =============================================================================


# -----------------------------------------------------------------------------
# 0. SETTINGS - run these lines first in every new terminal
# -----------------------------------------------------------------------------

# Region used by every command
$region = "ap-south-1"

# Account ID of the deploy identity (poc-cli): expect a 12-digit number
$acct = $null
$acct = aws sts get-caller-identity --query Account --output text
$acct

# Names derived from the account (the stack builds the lake bucket with this pattern)
$stack  = "aws-bytes-dq-arrival-check"
$lake   = "aws-bytes-lake-$acct-$region"
$deploy = "aws-bytes-deploy-$acct-$region"
$smLambda = "arn:aws:states:${region}:${acct}:stateMachine:aws-bytes-dq-compare"
$smGlue   = "arn:aws:states:${region}:${acct}:stateMachine:aws-bytes-dq-compare-glue-etl"

# Where the alert emails go: type your address when asked (SNS sends a confirmation link the first time)
$alertEmail = Read-Host "Alert email address"


# -----------------------------------------------------------------------------
# 1. CHECK THE FILES (local, no AWS calls)
# -----------------------------------------------------------------------------

# Work from the PoC folder
Set-Location C:\Users\risha\Desktop\Code\repo\aws-bytes\data-quality-arrival-check

# Lambda checker under its new file name, approach folder "lambda": expect 1 line
Select-String -Path .\lambda\lambda_checker.py -Pattern 'PATH_NAME = "lambda"'

# Glue job script in place: expect 1 line
Select-String -Path .\glue\glue_etl_job.py -Pattern 'APPROACH = "glue-etl"'

# Glue script is v5 (native Glue DQ rules): expect 2 lines (the docstring and the rule)
Select-String -Path .\glue\glue_etl_job.py -Pattern 'SchemaMatch "contract" = 1.0'

# Template has both approaches: expect 2 lines (the Lambda handler, the Glue script)
Select-String -Path .\infra\template.yaml -Pattern "Handler: lambda_checker.handler", "ScriptLocation: ../glue/glue_etl_job.py"

# The old file name is gone: expect False
Test-Path .\footer_checker_lambda.py


# -----------------------------------------------------------------------------
# 2. DEPLOY (as poc-cli) - stack update in place, previewed first; the Lander is not changed
#    Why the Lander-key step: "aws cloudformation package" zips every Lambda file again, and the zip
#    always gets a new name (the CLI copies the file without its timestamp, so each zip differs). Left as
#    is, the deploy would push the Lander's same code again. We point the package back at the Lander zip
#    that is already deployed, then PREVIEW the change set before applying anything.
# -----------------------------------------------------------------------------

# Stack must be healthy before the update: expect UPDATE_COMPLETE or CREATE_COMPLETE
aws cloudformation describe-stacks --stack-name $stack --region $region --query "Stacks[0].StackStatus" --output text

# Lander's last-modified time BEFORE the deploy (compare after: must be the same)
aws lambda get-function-configuration --function-name aws-bytes-dq-lander --region $region --query LastModified --output text

# Zip + upload the Lambda code and upload the Glue script. Must run FROM INSIDE infra\
Push-Location C:\Users\risha\Desktop\Code\repo\aws-bytes\data-quality-arrival-check\infra; aws cloudformation package --template-file template.yaml --s3-bucket $deploy --s3-prefix data-quality-arrival-check --output-template-file $env:TEMP\dq-packaged.yaml --region $region; Pop-Location

# Lander zip key in the DEPLOYED stack (the first S3Key after "LanderFunction:")
$deployed = (aws cloudformation get-template --stack-name $stack --region $region --query TemplateBody --output text) -join "`n"
$depKey = [regex]::Match($deployed, 'LanderFunction:[\s\S]*?S3Key:\s*(\S+)').Groups[1].Value

# Lander zip key in the NEW package
$pkg = [IO.File]::ReadAllText("$env:TEMP\dq-packaged.yaml")
$newKey = [regex]::Match($pkg, 'LanderFunction:[\s\S]*?S3Key:\s*(\S+)').Groups[1].Value

# Show both (both must be non-empty; they usually differ)
"deployed Lander key: $depKey"
"packaged Lander key: $newKey"

# Put the deployed key back into the package (exact text replace; UTF-8 without BOM)
[IO.File]::WriteAllText("$env:TEMP\dq-packaged.yaml", $pkg.Replace($newKey, $depKey), (New-Object System.Text.UTF8Encoding($false)))

# Check: the Lander's key in the package now equals the deployed one (expect True)
[regex]::Match([IO.File]::ReadAllText("$env:TEMP\dq-packaged.yaml"), 'LanderFunction:[\s\S]*?S3Key:\s*(\S+)').Groups[1].Value -eq $depKey

# Check: the Glue script location points at S3 (expect 1 line "ScriptLocation: s3://aws-bytes-deploy-...")
Select-String -Path $env:TEMP\dq-packaged.yaml -Pattern "ScriptLocation"

# PREVIEW: create a change set without applying it
aws cloudformation deploy --template-file $env:TEMP\dq-packaged.yaml --stack-name $stack --capabilities CAPABILITY_NAMED_IAM --parameter-overrides AlertEmail=$alertEmail --region $region --no-execute-changeset

# Keep the change set's ID
$cs = $null
$cs = aws cloudformation list-change-sets --stack-name $stack --region $region --query "Summaries[-1].ChangeSetId" --output text

# What it would change (action, resource, replaced?).
#   Expect Modify (Replacement False): GlueEtlJob (script), GlueEtlRole (DqHistory permission)
#   Also listed, same code/ARNs (repackaged Lambdas + resources that point at their ARNs): FooterCheckerFunction,
#     AlertNotifierFunction, AlertNotifierSchedulerRole, AlertNotifierSchedule, CompareStateMachineRole,
#     CompareStateMachine, RawObjectCreatedRule, RunnerRole, RuleInvokeCheckerPermission (Conditional)
#   Must NOT be listed: LanderFunction, LandStateMachine, LakeBucket, YellowRawTable, FileLogTable, any LogGroup
aws cloudformation describe-change-set --change-set-name $cs --region $region --query "Changes[].ResourceChange.[Action, LogicalResourceId, Replacement]" --output table

# STOP: read the table. Run the lines below only if it is what you expect.

# Apply exactly that change set
aws cloudformation execute-change-set --change-set-name $cs --region $region

# Wait until the update finishes (no output = done, ~1-2 min)
aws cloudformation wait stack-update-complete --stack-name $stack --region $region

# Stack status: expect UPDATE_COMPLETE
aws cloudformation describe-stacks --stack-name $stack --region $region --query "Stacks[0].StackStatus" --output text

# Lambda checker: expect lambda_checker.handler  2048  2048  300  Successful
aws lambda get-function-configuration --function-name aws-bytes-dq-footer-checker --region $region --query "[Handler, MemorySize, EphemeralStorage.Size, Timeout, LastUpdateStatus]" --output text

# Glue job: expect 5.0  G.1X  2  15  40
aws glue get-job --job-name aws-bytes-dq-glue-etl --region $region --query "Job.[GlueVersion, WorkerType, NumberOfWorkers, Timeout, ExecutionProperty.MaxConcurrentRuns]" --output text

# Lander's last-modified time AFTER: expect the same value as before
aws lambda get-function-configuration --function-name aws-bytes-dq-lander --region $region --query LastModified --output text

# Alert email confirmed (as the runner): expect a SubscriptionArn starting arn:aws:sns:... (not "PendingConfirmation")
aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:${region}:${acct}:aws-bytes-dq-alerts --profile dqrunner --region $region --query "Subscriptions[].[Endpoint, SubscriptionArn]" --output text


# -----------------------------------------------------------------------------
# 3. glue-etl TARGETED SMOKE, v5 (as the runner) - the fewest files that hit every check once
#      2019-01  all-empty airport_fee (must NOT fail SchemaMatch) + bad rows      -> passed_with_rejects
#      2023-02  5 type changes + Airport_fee spelling                              -> quarantined (SchemaMatch, names)
#      2025-01  new column cbd_congestion_fee (+ the 2023 changes)                 -> quarantined + ColumnCount note
#      planted  P1 unreadable · P2 cut off · P3 zero rows · P4 missing column · P5 same column twice
#               · P6 fare as text · P7 known bad rows · P10 size differs · P11 same file twice (2 files)
#    13 Glue runs. Cost estimate: 10 tiny planted files ~ $0.02 each (1-minute minimum) + 3 real files ~ $0.06
#    each = about $0.40. Lambda: its 93-file results from 21-Nov stay as they are (nothing re-run).
#    The planted uploads also fire the Lambda live path (cents); a re-uploaded file with the same bytes may show
#    there as skipped_duplicate - expected, not used for this comparison.
# -----------------------------------------------------------------------------

# Work from the PoC folder
Set-Location C:\Users\risha\Desktop\Code\repo\aws-bytes\data-quality-arrival-check

# Upload the planted files (one line each; source-bytes = what the "source" claims)
aws s3 cp .\planted\p1_damaged_footer.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p1_damaged_footer/yellow_tripdata_2019-03.parquet --metadata source-bytes=45673 --profile dqrunner --region $region
aws s3 cp .\planted\p2_cut_off_copy.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p2_cut_off_copy/yellow_tripdata_2019-03.parquet --metadata source-bytes=45875 --profile dqrunner --region $region
aws s3 cp .\planted\p3_zero_rows.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p3_zero_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=3331 --profile dqrunner --region $region
aws s3 cp .\planted\p4_missing_vendorid.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p4_missing_vendorid/yellow_tripdata_2019-03.parquet --metadata source-bytes=45477 --profile dqrunner --region $region
aws s3 cp .\planted\p5_duplicate_column.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p5_duplicate_column/yellow_tripdata_2019-03.parquet --metadata source-bytes=46183 --profile dqrunner --region $region
aws s3 cp .\planted\p6_fare_as_text.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p6_fare_as_text/yellow_tripdata_2019-03.parquet --metadata source-bytes=45683 --profile dqrunner --region $region
aws s3 cp .\planted\p7_tier2_known_bad_rows.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=45678 --profile dqrunner --region $region
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p10_size_differs/yellow_tripdata_2019-03.parquet --metadata source-bytes=99999 --profile dqrunner --region $region
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p11_delivered_twice/yellow_tripdata_2019-03.parquet --metadata source-bytes=45726 --profile dqrunner --region $region
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p11_delivered_twice/yellow_tripdata_2019-03_resent.parquet --metadata source-bytes=45726 --profile dqrunner --region $region

# Planted folder holds exactly these files: expect Total Objects: 10 (if more, STOP and paste the list)
aws s3 ls s3://$lake/raw/nyc-taxi/yellow-planted/ --recursive --summarize --profile dqrunner --region $region

# Inputs: one prefix per run (JSON through temp files so PowerShell quoting cannot break it)
@{ prefix = "raw/nyc-taxi/yellow/year=2019/month=01/" } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-s1.json -Encoding ascii
@{ prefix = "raw/nyc-taxi/yellow/year=2023/month=02/" } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-s2.json -Encoding ascii
@{ prefix = "raw/nyc-taxi/yellow/year=2025/month=01/" } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-s3.json -Encoding ascii
@{ prefix = "raw/nyc-taxi/yellow-planted/" } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-s4.json -Encoding ascii

# Clear first: a failed assignment in PowerShell keeps the OLD value
$s1 = $null; $s2 = $null; $s3 = $null; $s4 = $null

# Start the 4 measuring runs (13 Glue job runs in total)
$s1 = aws stepfunctions start-execution --state-machine-arn $smGlue --input file://$env:TEMP\dq-s1.json --profile dqrunner --region $region --query executionArn --output text
$s2 = aws stepfunctions start-execution --state-machine-arn $smGlue --input file://$env:TEMP\dq-s2.json --profile dqrunner --region $region --query executionArn --output text
$s3 = aws stepfunctions start-execution --state-machine-arn $smGlue --input file://$env:TEMP\dq-s3.json --profile dqrunner --region $region --query executionArn --output text
$s4 = aws stepfunctions start-execution --state-machine-arn $smGlue --input file://$env:TEMP\dq-s4.json --profile dqrunner --region $region --query executionArn --output text

# Status of all 4: repeat until none shows RUNNING (about 5-8 minutes). Read nothing else until then.
foreach ($e in $s1, $s2, $s3, $s4) { aws stepfunctions describe-execution --execution-arn $e --profile dqrunner --region $region --query status --output text }


# -----------------------------------------------------------------------------
# 4. SMOKE RESULTS (as the runner) - ONLY after section 3 shows no RUNNING
# -----------------------------------------------------------------------------

# Summaries incl. billed DPU-hours per run (used to estimate the full run's cost before you start it)
foreach ($e in $s1, $s2, $s3, $s4) { aws stepfunctions describe-execution --execution-arn $e --profile dqrunner --region $region --query "[status, output]" --output text }

# If any FAILED: its error
foreach ($e in $s1, $s2, $s3, $s4) { aws stepfunctions describe-execution --execution-arn $e --profile dqrunner --region $region --query cause --output text }

# Real files, lambda (21-Nov) vs glue-etl (now): status, rows rejected, per rule - both rows of a file must match
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "sort_by(Items[?(contains(file.S, 'year=2019/month=01/') || contains(file.S, 'year=2023/month=02/') || contains(file.S, 'year=2025/month=01/')) && (checker.S=='lambda#compare' || checker.S=='glue-etl#compare')], &file.S)[].[file.S, checker.S, status.S, rows_rejected.N, rejects_by_rule.M.negative_trip_duration.N, rejects_by_rule.M.pickup_outside_file_month.N]" --output table

# Real files, glue-etl reasons + notes (2025-01 expect a note: new column(s) not in the contract: cbd_congestion_fee)
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?(contains(file.S, 'year=2019/month=01/') || contains(file.S, 'year=2023/month=02/') || contains(file.S, 'year=2025/month=01/')) && checker.S=='glue-etl#compare'].[file.S, reasons.L[].S, notes.L[].S]" --output json

# Planted, glue-etl: file, status, rows rejected, reasons
#   expect P1 not readable · P2 FileSize + not readable · P3 RowCount · P4 ColumnExists (+ SchemaMatch) · P5 ambiguous
#   · P6 SchemaMatch (fare_amount) · P7 passed_with_rejects 14 · P10 FileSize · P11 both FileUniqueness
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "sort_by(Items[?starts_with(file.S, 'raw/nyc-taxi/yellow-planted/') && checker.S=='glue-etl#compare'], &file.S)[].[file.S, status.S, rows_rejected.N, reasons.L[].S]" --output json

# Every Glue DQ rule on 2019-01: ruleset, rule, outcome (all file rules Passed; the 2 row rules Failed)
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?file.S=='raw/nyc-taxi/yellow/year=2019/month=01/yellow_tripdata_2019-01.parquet' && checker.S=='glue-etl#compare'].dq_rules.L[].M.[ruleset.S, rule.S, outcome.S]" --output table

# Time per step (ms) on the 3 real files
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?(contains(file.S, 'year=2019/month=01/') || contains(file.S, 'year=2023/month=02/') || contains(file.S, 'year=2025/month=01/')) && checker.S=='glue-etl#compare'].[file.S, job_execution_s.N, step_ms]" --output json

# STOP: paste the output. The full run (section 5) starts only after these match and you OK its cost estimate.


# -----------------------------------------------------------------------------
# 5. glue-etl FULL MEASURING RUN, v5 (as the runner) - all 93 landed files. Only after section 4 matches lambda.
#    Re-checks the 3 smoke files too (their rows and outputs are overwritten with the same result).
#    Cost estimate: about $4.50-5 (smoke 21-Nov measured ~108 s per quarantined file and ~58 s extra per file with
#    bad rows vs the v4 run, which cost $3.54 measured).
#    Start + status only.
# -----------------------------------------------------------------------------

# Clear first
$glueRun = $null

# Start (no input = every file under raw/nyc-taxi/yellow/)
$glueRun = aws stepfunctions start-execution --state-machine-arn $smGlue --profile dqrunner --region $region --query executionArn --output text

# Print it
$glueRun

# Status: repeat until SUCCEEDED or FAILED (93 files, 40 at a time: about 10-15 minutes)
aws stepfunctions describe-execution --execution-arn $glueRun --profile dqrunner --region $region --query status --output text


# -----------------------------------------------------------------------------
# 6. glue-etl FULL RESULTS (as the runner) - ONLY after section 5 shows SUCCEEDED
# -----------------------------------------------------------------------------

# Run time: status, start, stop
aws stepfunctions describe-execution --execution-arn $glueRun --profile dqrunner --region $region --query "[status, startDate, stopDate]" --output text

# Summary (lambda 21-Nov: 49 passed_with_rejects, 44 quarantined, rejected 82,309 = 72,484 + 9,825)
aws stepfunctions describe-execution --execution-arn $glueRun --profile dqrunner --region $region --query output --output text

# Files where lambda and glue-etl disagree on status or rejected rows (expect nothing printed)
$both = $null
$both = aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?starts_with(file.S, 'raw/nyc-taxi/yellow/') && (checker.S=='lambda#compare' || checker.S=='glue-etl#compare')].{f: file.S, c: checker.S, s: status.S, r: rows_rejected.N}" --output json | ConvertFrom-Json
# (rows_rejected compared as text: quarantined rows have none on either side, and two empty values must count as equal)
$both | Group-Object f | Where-Object { ($_.Group.s | Select-Object -Unique).Count -ne 1 -or (@($_.Group | ForEach-Object { "$($_.r)" }) | Select-Object -Unique).Count -ne 1 } | ForEach-Object { $_.Name }

# Which Glue DQ rule failed, counted over the 93 files (rule text without the file path), saved for the README
$rules = $null
$rules = aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?starts_with(file.S, 'raw/nyc-taxi/yellow/') && checker.S=='glue-etl#compare'].dq_rules.L[].M.{set: ruleset.S, rule: rule.S, out: outcome.S}" --output json | ConvertFrom-Json
& { $rules | ForEach-Object { "{0} | {1} | {2}" -f $_.set, ($_.rule -replace '"s3://[^"]*"', '"<path>"' -replace '"[a-z_]+"$', '"<column>"'), $_.out } | Group-Object | Sort-Object Name | ForEach-Object { "{0,5}  {1}" -f $_.Count, $_.Name } } | Tee-Object -FilePath $env:TEMP\dq-glue-rules.txt

# Step times + job time for every file, saved for the README stats
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?starts_with(file.S, 'raw/nyc-taxi/yellow/') && checker.S=='glue-etl#compare'].{f: file.S, s: status.S, rows: num_rows.N, job_s: job_execution_s.N, steps: step_ms}" --output json | Set-Content -Path $env:TEMP\dq-glue-run-v5.json -Encoding utf8
explorer.exe $env:TEMP

# glue-etl outputs: expect validated 49, rejected 49, quarantine 44
aws s3 ls s3://$lake/validated/glue-etl/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/rejected/glue-etl/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/quarantine/glue-etl/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2


# -----------------------------------------------------------------------------
# 7. lambda MEASURING RUN (as the runner) - all 93 landed files with the renamed checker
#    Writes SK lambda#compare + validated|rejected|quarantine/lambda/. The old footer rows and folders stay
#    until section 11. Start + status only.
# -----------------------------------------------------------------------------

# Clear first
$lambdaRun = $null

# Start (no input = every file under raw/nyc-taxi/yellow/)
$lambdaRun = aws stepfunctions start-execution --state-machine-arn $smLambda --profile dqrunner --region $region --query executionArn --output text

# Print it (must contain :execution:aws-bytes-dq-compare:)
$lambdaRun

# Status: repeat until SUCCEEDED or FAILED (about 1 minute)
aws stepfunctions describe-execution --execution-arn $lambdaRun --profile dqrunner --region $region --query status --output text


# -----------------------------------------------------------------------------
# 8. lambda RESULTS (as the runner) - ONLY after section 7 shows SUCCEEDED
# -----------------------------------------------------------------------------

# Run time + summary
aws stepfunctions describe-execution --execution-arn $lambdaRun --profile dqrunner --region $region --query "[status, startDate, stopDate]" --output text
aws stepfunctions describe-execution --execution-arn $lambdaRun --profile dqrunner --region $region --query output --output text

# lambda outputs: expect validated 49, rejected 49, quarantine 44
aws s3 ls s3://$lake/validated/lambda/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/rejected/lambda/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/quarantine/lambda/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2

# Dropped log fields are not written any more: expect 0
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "length(Items[?checker.S=='lambda#compare' && (null_counts || pickup_min || pickup_max || num_row_groups || created_by || source_etag)])"


# -----------------------------------------------------------------------------
# 9. PLANTED TESTS (as the runner). Files in .\planted\ (labelled PLANTED: synthetic rows in the 2019 contract
#    schema, month 2019-03, 1,000 rows each). Each upload lands under raw/nyc-taxi/yellow-planted/<case>/ and
#    fires the LIVE path (lambda approach). The glue-etl approach checks the same files with a measuring run.
#    --metadata source-bytes = what a real landing stamps; the size check compares the landed size with it.
# -----------------------------------------------------------------------------

# Work from the PoC folder
Set-Location C:\Users\risha\Desktop\Code\repo\aws-bytes\data-quality-arrival-check

# P1 damaged footer (bytes inside the footer overwritten, size intact) -> expect quarantined: not readable Parquet
aws s3 cp .\planted\p1_damaged_footer.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p1_damaged_footer/yellow_tripdata_2019-03.parquet --metadata source-bytes=45673 --profile dqrunner --region $region

# P2 cut-off copy (last 1,000 bytes missing; source said 45,875) -> expect quarantined: size differs + not readable
aws s3 cp .\planted\p2_cut_off_copy.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p2_cut_off_copy/yellow_tripdata_2019-03.parquet --metadata source-bytes=45875 --profile dqrunner --region $region

# P3 zero rows -> expect quarantined: 0 rows (glue-etl: Glue DQ rule RowCount > 0 failed)
aws s3 cp .\planted\p3_zero_rows.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p3_zero_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=3331 --profile dqrunner --region $region

# P4 VendorID column removed -> expect quarantined: missing column vendorid (glue-etl: ColumnExists "vendorid" failed)
aws s3 cp .\planted\p4_missing_vendorid.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p4_missing_vendorid/yellow_tripdata_2019-03.parquet --metadata source-bytes=45477 --profile dqrunner --region $region

# P5 airport_fee AND Airport_fee in one file -> expect quarantined: ambiguous columns
aws s3 cp .\planted\p5_duplicate_column.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p5_duplicate_column/yellow_tripdata_2019-03.parquet --metadata source-bytes=46183 --profile dqrunner --region $region

# P6 fare_amount stored as text -> expect quarantined: type fare_amount string differs from the contract (double)
aws s3 cp .\planted\p6_fare_as_text.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p6_fare_as_text/yellow_tripdata_2019-03.parquet --metadata source-bytes=45683 --profile dqrunner --region $region

# P7 Tier 2 known answer (14 bad rows planted, 3 edge rows that must be kept)
#    -> expect passed_with_rejects: 14 rejected (negative_trip_duration 9, pickup_outside_file_month 7), 986 valid
aws s3 cp .\planted\p7_tier2_known_bad_rows.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=45678 --profile dqrunner --region $region

# P10 size differs but the file is readable (a clean file; source claims 99,999 B)
#    -> expect quarantined by both: lambda "size differs"; glue-etl Glue DQ FileSize failed
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p10_size_differs/yellow_tripdata_2019-03.parquet --metadata source-bytes=99999 --profile dqrunner --region $region

# P11 the same content delivered twice under two names in one folder (two uploads)
#    -> glue-etl: both quarantined, Glue DQ FileUniqueness failed. lambda: both passed (its duplicate check is
#       "same key delivered again" = P8 below; two different names are two different files to it)
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p11_delivered_twice/yellow_tripdata_2019-03.parquet --metadata source-bytes=45726 --profile dqrunner --region $region
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p11_delivered_twice/yellow_tripdata_2019-03_resent.parquet --metadata source-bytes=45726 --profile dqrunner --region $region

# Wait ~1 minute, then (lambda, live path): one row per planted file (file, status, rows rejected, first reason, alert)
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?starts_with(file.S, 'raw/nyc-taxi/yellow-planted/') && checker.S=='lambda'].[file.S, status.S, rows_rejected.N, reasons.L[0].S, alert_status.S]" --output table

# glue-etl on the same 10 planted files (before P8/P9 change the P7 key). Cost: 10 runs x 1-minute minimum, about $0.20-0.40
$planted = $null
@{ prefix = "raw/nyc-taxi/yellow-planted/" } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-glue-planted.json -Encoding ascii
$planted = aws stepfunctions start-execution --state-machine-arn $smGlue --input file://$env:TEMP\dq-glue-planted.json --profile dqrunner --region $region --query executionArn --output text
$planted

# Status: repeat until SUCCEEDED or FAILED
aws stepfunctions describe-execution --execution-arn $planted --profile dqrunner --region $region --query status --output text

# ONLY after SUCCEEDED: glue-etl rows for the planted files
#   expect the same statuses and counts as the lambda rows above, except P11 (glue-etl quarantined, lambda passed)
#   first reason per file: P1 not readable · P2 FileSize · P3 RowCount · P4 ColumnExists · P5 ambiguous columns
#   · P6 SchemaMatch (fare_amount) · P10 FileSize · P11 FileUniqueness
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?starts_with(file.S, 'raw/nyc-taxi/yellow-planted/') && checker.S=='glue-etl#compare'].[file.S, status.S, rows_rejected.N, reasons.L[0].S, alert_status.S]" --output table

# P8 the SAME P7 file delivered again (same bytes, same S3 checksum) -> expect skipped_duplicate (duplicate_count 1). lambda only.
aws s3 cp .\planted\p7_tier2_known_bad_rows.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=45678 --profile dqrunner --region $region

# Wait ~1 minute, then: duplicate count + the duplicate run's ID on P7's row (expect 1 and one request ID)
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?file.S=='raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet' && checker.S=='lambda'].[status.S, duplicate_count.N, duplicate_run_ids.L[].S]" --output json

# P9 re-publish: a corrected file under P7's key (all rows clean, new checksum) -> expect passed; P7's rejected copy deleted. lambda only.
aws s3 cp .\planted\p9_republish_clean.parquet s3://$lake/raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet --metadata source-bytes=45726 --profile dqrunner --region $region

# Wait ~1 minute, then: P7 key now passed with 0 rejected
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "Items[?file.S=='raw/nyc-taxi/yellow-planted/p7_tier2_rows/yellow_tripdata_2019-03.parquet' && checker.S=='lambda'].[status.S, rows_rejected.N]" --output text

# lambda planted outputs: quarantine expects 7 objects (P1-P6, P10); rejected expects 0 (P9 removed P7's); validated expects 3 (P7 key + the 2 P11 files)
aws s3 ls s3://$lake/quarantine/lambda/nyc-taxi/yellow-planted/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/rejected/lambda/nyc-taxi/yellow-planted/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2
aws s3 ls s3://$lake/validated/lambda/nyc-taxi/yellow-planted/ --recursive --summarize --profile dqrunner --region $region | Select-Object -Last 2

# Events the live checker never processed (expect 0)
aws sqs get-queue-attributes --queue-url https://sqs.${region}.amazonaws.com/${acct}/aws-bytes-dq-footer-checker-failed --attribute-names ApproximateNumberOfMessages --profile dqrunner --region $region --query "Attributes.ApproximateNumberOfMessages" --output text


# -----------------------------------------------------------------------------
# 10. ALERT EMAIL (as the runner) - run the alert notifier now instead of waiting up to 15 minutes
#     One email per approach (lambda, glue-etl), each with its own Alert ID ending in the approach; rows -> "sent".
# -----------------------------------------------------------------------------

# Run the alert notifier now; its reply is written to a temp file
aws lambda invoke --function-name aws-bytes-dq-alert-notifier --profile dqrunner --region $region $env:TEMP\dq-alert-out.json

# What it did: pending found + one entry per email (approach, alert_id, rows, marked_sent); 0 pending = already sent
Get-Content $env:TEMP\dq-alert-out.json

# Nothing left to report: expect 0
aws dynamodb scan --table-name aws-bytes-dq-file-log --profile dqrunner --region $region --query "length(Items[?alert_status.S=='pending'])"


# -----------------------------------------------------------------------------
# 11. FULL CLEAN FOR THE FINAL RUN (as poc-cli) - DATA DELETE. Only when you decide to do the final run.
#     Deletes: every approach's folders (incl. the old footer/), _tmp/, the planted raw files, every file-log row.
#     Keeps:   raw/nyc-taxi/yellow/ (93 landed files), the Glue table and partitions, the stack.
#     Lines are commented out on purpose: remove the "# " only when you decide.
# -----------------------------------------------------------------------------

# foreach ($z in "validated", "rejected", "quarantine") { foreach ($a in "footer", "lambda", "glue-etl") { aws s3 rm s3://$lake/$z/$a/ --recursive --region $region --quiet } }
# aws s3 rm s3://$lake/_tmp/ --recursive --region $region --quiet
# aws s3 rm s3://$lake/raw/nyc-taxi/yellow-planted/ --recursive --region $region --quiet
# $rows = $null
# $rows = aws dynamodb scan --table-name aws-bytes-dq-file-log --region $region --query "Items[].[file.S, checker.S]" --output text
# ($rows | Measure-Object).Count
# foreach ($r in $rows) { $f, $c = $r -split "`t"; @{ file = @{ S = $f }; checker = @{ S = $c } } | ConvertTo-Json -Compress | Set-Content -Path $env:TEMP\dq-key.json -Encoding ascii; aws dynamodb delete-item --table-name aws-bytes-dq-file-log --key file://$env:TEMP\dq-key.json --region $region }

# After the clean: rows left (expect 0) and raw/ untouched (expect Total Objects: 93, Total Size: 5450358081)
# aws dynamodb scan --table-name aws-bytes-dq-file-log --region $region --query "length(Items)"
# aws s3 ls s3://$lake/raw/nyc-taxi/yellow/ --recursive --summarize --region $region | Select-Object -Last 2


# -----------------------------------------------------------------------------
# 12. FINAL PARALLEL RUN (as the runner) - after section 11. Both measuring runs at the same time.
#     Then read results with sections 6 and 8 (using $glueRun and $lambdaRun), planted tests with section 9.
# -----------------------------------------------------------------------------

# $glueRun = $null; $lambdaRun = $null
# $glueRun = aws stepfunctions start-execution --state-machine-arn $smGlue --profile dqrunner --region $region --query executionArn --output text
# $lambdaRun = aws stepfunctions start-execution --state-machine-arn $smLambda --profile dqrunner --region $region --query executionArn --output text
# aws stepfunctions describe-execution --execution-arn $lambdaRun --profile dqrunner --region $region --query status --output text
# aws stepfunctions describe-execution --execution-arn $glueRun --profile dqrunner --region $region --query status --output text


# -----------------------------------------------------------------------------
# 13. LAND RUN - DO NOT RUN BEFORE THE POC ENDS
#     With no input it lands 2019-01 up to the current month: a newly published TLC month would land,
#     fire the live checker and change every count above.
# -----------------------------------------------------------------------------

# $exec = $null
# $exec = aws stepfunctions start-execution --state-machine-arn arn:aws:states:${region}:${acct}:stateMachine:aws-bytes-dq-land --profile dqrunner --region $region --query executionArn --output text
# aws stepfunctions describe-execution --execution-arn $exec --profile dqrunner --region $region --query status --output text


# -----------------------------------------------------------------------------
# 14. TEARDOWN - END OF THE POC ONLY (as poc-cli). Deletes raw/, the Lander and every result.
#     Lines are commented out on purpose: remove the "# " only when you decide to tear down.
# -----------------------------------------------------------------------------

# Empty the lake bucket (a bucket must be empty to be deleted)
# aws s3 rm s3://$lake/ --recursive --region $region

# Delete the stack: bucket, Glue database + table, file log, Lambdas, Glue job, state machines,
# EventBridge rule, SQS queue, Lambda log groups, all its roles (incl. poc-dq-runner)
# aws cloudformation delete-stack --stack-name $stack --region $region
# aws cloudformation wait stack-delete-complete --stack-name $stack --region $region

# Glue creates its job log groups itself (not part of the stack): list them, then delete each one listed
# aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs --region $region --query "logGroups[].logGroupName" --output text
# aws logs delete-log-group --log-group-name /aws-glue/jobs/output --region $region
# aws logs delete-log-group --log-group-name /aws-glue/jobs/error --region $region

# Old Lambda zips + Glue script in the deploy bucket (the bucket itself stays for other PoCs)
# aws s3 rm s3://$deploy/data-quality-arrival-check/ --recursive --region $region