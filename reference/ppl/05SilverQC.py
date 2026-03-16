# Databricks notebook source
# MAGIC %md
# MAGIC Purpose
# MAGIC
# MAGIC This notebook performs data quality validation on the Silver-layer tables produced during the transformation step. The goal is to ensure that submitted study data follow the expected data types, formats, and value ranges defined in the GEMS rulebook before the data can proceed to downstream processing and Gold-layer publication.
# MAGIC
# MAGIC While earlier pipeline steps verify whether the required datasets were delivered (Stoplight validation), this notebook evaluates whether the actual values inside the datasets are valid and usable for analysis.
# MAGIC
# MAGIC Main Tasks
# MAGIC 1. Load the Rulebook
# MAGIC
# MAGIC The notebook reads the GEMS rulebook, which defines validation rules for each sheet and column in the data entry template.
# MAGIC These rules include:
# MAGIC
# MAGIC Expected data types (string, numeric, date, boolean)
# MAGIC
# MAGIC Required fields
# MAGIC
# MAGIC Allowed value ranges
# MAGIC
# MAGIC Format constraints
# MAGIC
# MAGIC The rulebook acts as the central definition of data validation rules used across all studies.
# MAGIC
# MAGIC 2. Validate Data Types
# MAGIC
# MAGIC Each column is checked against its expected type defined in the rulebook.
# MAGIC
# MAGIC Examples include:
# MAGIC
# MAGIC Numeric columns must contain valid numbers
# MAGIC
# MAGIC Date columns must follow accepted date formats
# MAGIC
# MAGIC Boolean fields must contain allowed logical values
# MAGIC
# MAGIC Text fields are accepted as free-form strings
# MAGIC
# MAGIC Invalid values that cannot be converted to the required type are flagged as QC violations.
# MAGIC
# MAGIC 3. Validate Date Formats
# MAGIC
# MAGIC Date fields are checked to ensure they follow accepted formats used in the template.
# MAGIC
# MAGIC Supported formats include:
# MAGIC
# MAGIC YYYY-MM-DD
# MAGIC
# MAGIC MM/DD/YYYY
# MAGIC
# MAGIC M/D/YYYY
# MAGIC
# MAGIC If timestamps are included (for example YYYY-MM-DD 00:00:00), the date portion is extracted and validated.
# MAGIC
# MAGIC Entries that cannot be interpreted as valid dates are flagged.
# MAGIC
# MAGIC 4. Validate Numeric Ranges
# MAGIC
# MAGIC Where defined in the rulebook, numeric variables are checked against minimum and maximum allowable values.
# MAGIC
# MAGIC Examples include:
# MAGIC
# MAGIC intake values must be non-negative
# MAGIC
# MAGIC production variables must fall within realistic biological ranges
# MAGIC
# MAGIC Values outside acceptable ranges are recorded as QC issues.
# MAGIC
# MAGIC 5. Validate Required Fields
# MAGIC
# MAGIC Columns marked as Must have in the rulebook are checked for missing values.
# MAGIC
# MAGIC If a required field is empty or null, it is flagged as a validation issue.
# MAGIC Optional fields may remain empty without triggering a failure.
# MAGIC
# MAGIC 6. Record QC Results
# MAGIC
# MAGIC Validation results are written to two operational tables.
# MAGIC
# MAGIC silverQcSummary
# MAGIC
# MAGIC This table stores one record per study submission, summarizing whether the study passed or failed QC validation.
# MAGIC
# MAGIC Example fields include:
# MAGIC
# MAGIC studyId
# MAGIC
# MAGIC sequence
# MAGIC
# MAGIC qcStatus (PASS or FAIL)
# MAGIC
# MAGIC qcMessage
# MAGIC
# MAGIC ruleVersionUsed
# MAGIC
# MAGIC silverQcDetails
# MAGIC
# MAGIC This table stores detailed validation violations, including the exact location of each issue.
# MAGIC
# MAGIC Example fields include:
# MAGIC
# MAGIC studyId
# MAGIC
# MAGIC sequence
# MAGIC
# MAGIC sourceSheet
# MAGIC
# MAGIC columnName
# MAGIC
# MAGIC rowNumber
# MAGIC
# MAGIC ruleName
# MAGIC
# MAGIC invalidValue
# MAGIC
# MAGIC These detailed records are used later to generate clear QC feedback messages for contributors.
# MAGIC
# MAGIC Output and Next Step
# MAGIC
# MAGIC The outputs from this notebook are used by Notebook 06 – Notifications, which generates email reports summarizing any validation issues. Contributors are then asked to correct the identified problems and update their workbook before resubmission.
# MAGIC
# MAGIC Studies that pass all QC checks can proceed to Gold-layer processing, where finalized datasets are prepared for analysis and integration.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Config + helpers

# COMMAND ----------

from datetime import datetime, timezone
import re
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType
)

# -----------------------------
# Config
# -----------------------------
catalog = "gems_catalog"
schema  = "gems_schema"

RULE_VERSION = "v1.0"   # <-- change to "v2.0" later

# Exclude sheets from QC (Contributor is metadata, not experimental data)
QC_EXCLUDED_SHEETS_NORM = { "contributor" }

# Missing markers -> normalize to empty string (same as Silver Transform)
MISSING_MARKERS = {
    "", " ", ".", "-", "--",

    "na", "NA",
    "n/a", "N/A",
    "nan", "NaN", "NAN",
    "null", "NULL",
    "none", "None", "NONE",

    "missing", "Missing",
    "not available", "Not available", "NOT AVAILABLE",

    "nd", "ND",
    "n.d.", "N.D.",
    "not recorded", "Not Recorded", "NOT RECORDED"
}

stoplightTable = f"{catalog}.{schema}.gemsValidationStoplight"
silverPrefix   = f"{catalog}.{schema}.silver"

# Rulebook file in UC Volume
rulebookPath  = f"/Volumes/{catalog}/{schema}/blob_gems_data/opsRulebook.xlsx"
rulebookSheet = "opsRulebookFields"

# Delta table snapshot of the rulebook (rebuilt each run)
rulebookDeltaTable = f"{catalog}.{schema}.opsRulebookFields"

# QC outputs (Delta)
qcDetailsTable = f"{catalog}.{schema}.silverQcDetails"
qcSummaryTable = f"{catalog}.{schema}.silverQcSummary"
qcLogTable     = f"{catalog}.{schema}.opsSilverQcLog"

qcRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def list_silver_tables() -> list:
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    out = []
    for r in rows:
        t = r["tableName"]
        tl = t.lower()

        # include only silver sheet tables
        if not tl.startswith("silver"):
            continue

        # exclude QC output tables and anything not a sheet
        if tl.startswith("silverqc"):
            continue

        out.append(f"{catalog}.{schema}.{t}")
    return sorted(out)

def sheet_from_silver_table(full_table: str) -> str:
    t = full_table.split(".")[-1]
    return t[len("silver"):] if t.lower().startswith("silver") else t

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Core-pass studies from Stoplight (latest run)

# COMMAND ----------

stop = spark.table(stoplightTable)

latestStoplightRun = (
    stop.select(F.max("gateRunId").alias("gateRunId"))
        .collect()[0]["gateRunId"]
)

corePassStudies = (
    stop.filter(
        (F.col("gateRunId") == latestStoplightRun) &
        (F.col("coreDataStatus") == "PASS")
    )
    .select(
        "studyId", "sequence",
        "coreDataStatus", "promisedDataStatus", "status",
        "coreDataMessage", "promisedDataMessage",
        "gateRunId"
    )
    .dropDuplicates()
)

passStudies = corePassStudies.select("studyId", "sequence").dropDuplicates()

print("QC runId:", qcRunId)
print("Latest stoplight gateRunId:", latestStoplightRun)
print("Core PASS (studyId, sequence):", passStudies.count())
display(corePassStudies.orderBy("studyId","sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Load opsRulebook.xlsx, filter RULE_VERSION, write to Delta
# MAGIC ###### This “turns it into a Delta table” so QC can be stable and queryable.

# COMMAND ----------

rule_pdf = pd.read_excel(
    rulebookPath,
    sheet_name=rulebookSheet,
    engine="openpyxl",
    dtype=str,
    keep_default_na=False
)
rule_pdf.columns = [str(c).strip() for c in rule_pdf.columns]

expected_cols = [
    "sourceSheet","columnName","isRequired","dataType",
    "minValue","maxValue","rangByUnit","templateUnitDefault","ruleVersion"
]
missing = [c for c in expected_cols if c not in rule_pdf.columns]
assert not missing, f"opsRulebookFields is missing columns: {missing}"

# Filter to active rule version
rule_pdf["ruleVersion"] = rule_pdf["ruleVersion"].astype(str).str.strip()
rule_pdf = rule_pdf[rule_pdf["ruleVersion"] == RULE_VERSION].copy()

print(f"Rulebook loaded from Excel: {len(rule_pdf)} rows for {RULE_VERSION}")

ruleDf = spark.createDataFrame(rule_pdf)

# Add normalized matching keys + stamp the run/version used
ruleDf = (
    ruleDf
    .withColumn("sourceSheetNorm", F.regexp_replace(F.lower(F.col("sourceSheet")), r"[^a-z0-9]+", ""))
    .withColumn("columnNameNorm", F.regexp_replace(F.lower(F.col("columnName")), r"[^a-z0-9]+", ""))
    .withColumn("ruleVersionUsed", F.lit(RULE_VERSION))
    .withColumn("loadedTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))
)

# Overwrite the Delta snapshot each run (small table, safest)
(ruleDf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(rulebookDeltaTable)
)

print("Rulebook snapshot written to Delta:", rulebookDeltaTable)
display(spark.table(rulebookDeltaTable).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — QC functions (type + range) and issue collector
# MAGIC ###### This enforces the rulebook columns against Silver.

# COMMAND ----------

rulebook = spark.table(rulebookDeltaTable)

# Helper: interpret isRequired as boolean
def is_required_col(c):
    # in your file, isRequired might be "1"/"0", "TRUE"/"FALSE", "Must have"
    return (
        F.lower(F.trim(F.col(c))).isin("1","true","yes","y","musthave","must")
    )

def normalize_missing_markers(df):
    missing_list = [m.lower() for m in MISSING_MARKERS]
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
            df = df.withColumn(c, F.when(F.col(c).isNull(), F.lit("")).otherwise(F.col(c)))
            df = df.withColumn(c, F.when(F.lower(F.col(c)).isin(missing_list), F.lit("")).otherwise(F.col(c)))
    return df

def try_cast_double(col):
    # Spark SQL try_cast works in Databricks runtimes
    return F.expr(f"try_cast({col} as double)")

def valid_date_expr(colname: str):
    c = F.trim(F.col(colname)).cast("string")
    c10 = F.when(F.length(c) >= 10, F.substring(c, 1, 10)).otherwise(c)

    return (
        F.to_date(c10, "yyyy-MM-dd").isNotNull() |
        F.to_date(c10, "MM/dd/yyyy").isNotNull() |
        F.to_date(c10, "M/d/yyyy").isNotNull()
    )

def build_type_fail_condition(dtype: str, colname: str):
    dt = str(dtype).strip().lower()

    # Empty is allowed (missingness handled separately by required check)
    not_empty = (F.col(colname).isNotNull() & (F.trim(F.col(colname)) != ""))

    if dt in ["string", "text"]:
        return F.lit(False)  # strings always ok
    if dt in ["int", "integer", "long"]:
        return not_empty & try_cast_double(colname).isNull()
    if dt in ["float", "double", "numeric", "number", "decimal"]:
        return not_empty & try_cast_double(colname).isNull()
    if dt in ["date"]:
        return not_empty & (~valid_date_expr(colname))
    if dt in ["bool", "boolean"]:
        return not_empty & (~F.lower(F.col(colname)).isin("0","1","true","false","yes","no","y","n"))
    # Unknown type: do not fail, just skip type check
    return F.lit(False)

def build_range_fail_condition(minv: str, maxv: str, colname: str):
    # Range checks only apply to non-empty values that can be cast to double
    not_empty = (F.col(colname).isNotNull() & (F.trim(F.col(colname)) != ""))
    x = try_cast_double(colname)
    cond = F.lit(False)

    mv = str(minv).strip() if minv is not None else ""
    xv = str(maxv).strip() if maxv is not None else ""

    if mv != "":
        cond = cond | (not_empty & x.isNotNull() & (x < F.lit(float(mv))))
    if xv != "":
        cond = cond | (not_empty & x.isNotNull() & (x > F.lit(float(xv))))
    return cond

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Run QC across Silver tables for core-pass studies
# MAGIC ###### Outputs:
# MAGIC
# MAGIC silverQcDetails: row counts of failures by check
# MAGIC
# MAGIC silverQcSummary: PASS/FAIL per study + message
# MAGIC
# MAGIC opsSilverQcLog: run metadata

# COMMAND ----------

# ---------- helper: compress row numbers ----------
def compress_rows(rows):
    if not rows:
        return ""

    rows = sorted(rows)
    ranges = []
    start = prev = rows[0]

    for r in rows[1:]:
        if r == prev + 1:
            prev = r
        else:
            ranges.append((start, prev))
            start = prev = r

    ranges.append((start, prev))

    parts = []
    for s, e in ranges:
        if s == e:
            parts.append(str(s))
        else:
            parts.append(f"{s}-{e}")

    return ", ".join(parts)

# ---------- discover silver tables ----------
silver_tables = list_silver_tables()

print("Silver tables discovered:", len(silver_tables))
for t in silver_tables:
    print(" -", t)

pass_keys = passStudies.collect()

qc_detail_rows = []
qc_log_rows = []

# ---------- main QC loop ----------
for st in silver_tables:

    sheetName = sheet_from_silver_table(st)
    sheetNorm = norm(sheetName)

    if sheetNorm in QC_EXCLUDED_SHEETS_NORM:

        qc_log_rows.append({
            "qcRunId": qcRunId,
            "ruleVersionUsed": RULE_VERSION,
            "silverTable": st,
            "sourceSheet": sheetName,
            "status": "SKIPPED_EXCLUDED",
            "errorMessage": "Sheet excluded from QC (metadata-only)",
            "createdTsUtc": datetime.now(timezone.utc).isoformat()
        })

        continue

    rb = rulebook.filter(F.col("sourceSheetNorm") == F.lit(sheetNorm))
    rb_count = rb.count()

    if rb_count == 0:

        qc_log_rows.append({
            "qcRunId": qcRunId,
            "ruleVersionUsed": RULE_VERSION,
            "silverTable": st,
            "sourceSheet": sheetName,
            "status": "SKIPPED_NO_RULEBOOK",
            "errorMessage": "",
            "createdTsUtc": datetime.now(timezone.utc).isoformat()
        })

        continue

    try:

        sdf_all = spark.table(st)
        sdf_all = normalize_missing_markers(sdf_all)

        rb_rows = rb.select(
            "sourceSheet",
            "columnName",
            "isRequired",
            "dataType",
            "minValue",
            "maxValue"
        ).collect()

        for k in pass_keys:

            sid = str(k["studyId"])
            seq = str(k["sequence"])

            sdf = sdf_all.filter(
                (F.col("studyId") == sid) &
                (F.col("sequence") == seq)
            )

            if sdf.rdd.isEmpty():
                continue

            # ---------- Required checks ----------
            for r in rb_rows:

                colName = str(r["columnName"]).strip()
                req_raw = str(r["isRequired"]).strip()
                req = (req_raw.lower() in ["1","true","yes","y","musthave","must"])

                if req:

                    if colName not in sdf.columns:

                        qc_detail_rows.append({
                            "qcRunId": qcRunId,
                            "ruleVersionUsed": RULE_VERSION,
                            "studyId": sid,
                            "sequence": seq,
                            "sourceSheet": sheetName,
                            "columnName": colName,
                            "checkType": "REQUIRED_COLUMN_MISSING",
                            "badRowCount": "ALL",
                            "message": f"Required column missing: {colName}",
                            "createdTsUtc": datetime.now(timezone.utc).isoformat()
                        })

                        continue

                    nonempty = sdf.filter(
                        ~(F.col(colName).isNull() | (F.trim(F.col(colName)) == ""))
                    ).count()

                    if nonempty == 0:

                        qc_detail_rows.append({
                            "qcRunId": qcRunId,
                            "ruleVersionUsed": RULE_VERSION,
                            "studyId": sid,
                            "sequence": seq,
                            "sourceSheet": sheetName,
                            "columnName": colName,
                            "checkType": "REQUIRED_COLUMN_EMPTY",
                            "badRowCount": str(sdf.count()),
                            "message": f"Required column has no values: {colName}",
                            "createdTsUtc": datetime.now(timezone.utc).isoformat()
                        })

            # ---------- TYPE + RANGE checks ----------
            for r in rb_rows:

                colName = str(r["columnName"]).strip()
                dtype   = str(r["dataType"]).strip()
                minv    = str(r["minValue"]).strip() if r["minValue"] is not None else ""
                maxv    = str(r["maxValue"]).strip() if r["maxValue"] is not None else ""

                if colName not in sdf.columns:
                    continue

                # TYPE CHECK
                type_fail = build_type_fail_condition(dtype, colName)

                bad_type_rows = (
                    sdf
                    .withColumn(
                        "row_number",
                        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
                    )
                    .filter(type_fail)
                    .select("row_number")
                    .collect()
                )

                if bad_type_rows:

                    rows = [r["row_number"] + 1 for r in bad_type_rows]
                    row_range = compress_rows(rows)

                    qc_detail_rows.append({
                        "qcRunId": qcRunId,
                        "ruleVersionUsed": RULE_VERSION,
                        "studyId": sid,
                        "sequence": seq,
                        "sourceSheet": sheetName,
                        "columnName": colName,
                        "checkType": "TYPE_MISMATCH",
                        "badRowCount": str(len(rows)),
                        "message": f"Type mismatch rows {row_range} for {colName} (expected {dtype})",
                        "createdTsUtc": datetime.now(timezone.utc).isoformat()
                    })

                # RANGE CHECK
                if minv != "" or maxv != "":

                    try:

                        range_fail = build_range_fail_condition(minv, maxv, colName)

                        bad_rows_df = (
                            sdf
                            .withColumn(
                                "row_number",
                                F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
                            )
                            .filter(range_fail)
                            .select("row_number")
                        )

                        bad_rows = [r["row_number"] + 1 for r in bad_rows_df.collect()]

                        if bad_rows:

                            row_range = compress_rows(bad_rows)

                            qc_detail_rows.append({
                                "qcRunId": qcRunId,
                                "ruleVersionUsed": RULE_VERSION,
                                "studyId": sid,
                                "sequence": seq,
                                "sourceSheet": sheetName,
                                "columnName": colName,
                                "checkType": "RANGE_VIOLATION",
                                "badRowCount": str(len(bad_rows)),
                                "message": f"Range violation rows {row_range} for {colName} (min={minv or 'NA'}, max={maxv or 'NA'})",
                                "createdTsUtc": datetime.now(timezone.utc).isoformat()
                            })

                    except Exception:
                        pass

        qc_log_rows.append({
            "qcRunId": qcRunId,
            "ruleVersionUsed": RULE_VERSION,
            "silverTable": st,
            "sourceSheet": sheetName,
            "status": "SUCCESS",
            "errorMessage": "",
            "createdTsUtc": datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:

        qc_log_rows.append({
            "qcRunId": qcRunId,
            "ruleVersionUsed": RULE_VERSION,
            "silverTable": st,
            "sourceSheet": sheetName,
            "status": "FAILED_TABLE_LEVEL",
            "errorMessage": str(e),
            "createdTsUtc": datetime.now(timezone.utc).isoformat()
        })

# ---------- build QC details DF ----------
detailSchema = StructType([
    StructField("qcRunId", StringType(), True),
    StructField("ruleVersionUsed", StringType(), True),
    StructField("studyId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("sourceSheet", StringType(), True),
    StructField("columnName", StringType(), True),
    StructField("checkType", StringType(), True),
    StructField("badRowCount", StringType(), True),
    StructField("message", StringType(), True),
    StructField("createdTsUtc", StringType(), True),
])

qcDetailsDf = spark.createDataFrame(qc_detail_rows, schema=detailSchema)

# ---------- summary per study ----------
if qcDetailsDf.rdd.isEmpty():

    qcSummaryDf = passStudies.withColumn("qcRunId", F.lit(qcRunId)) \
        .withColumn("ruleVersionUsed", F.lit(RULE_VERSION)) \
        .withColumn("qcStatus", F.lit("PASS")) \
        .withColumn("qcMessage", F.lit("OK")) \
        .withColumn("createdTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))

else:

    ranked = (
        qcDetailsDf
        .withColumn(
            "issueStr",
            F.concat_ws(": ", F.col("sourceSheet"), F.col("columnName"), F.col("message"))
        )
    )

    agg = (
        ranked.groupBy("studyId","sequence")
        .agg(
            F.count("*").alias("issueCount"),
            F.collect_list("issueStr").alias("issues")
        )
        .withColumn("qcStatus", F.lit("FAIL"))
        .withColumn(
            "qcMessage",
            F.concat(F.lit("Issues: "), F.concat_ws(" | ", F.slice(F.col("issues"),1,5)))
        )
        .withColumn("qcRunId", F.lit(qcRunId))
        .withColumn("ruleVersionUsed", F.lit(RULE_VERSION))
        .withColumn("createdTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))
        .select("qcRunId","ruleVersionUsed","studyId","sequence","qcStatus","qcMessage","issueCount","createdTsUtc")
    )

    qcSummaryDf = (
        passStudies
        .join(agg, on=["studyId","sequence"], how="left")
        .withColumn("qcRunId", F.lit(qcRunId))
        .withColumn("ruleVersionUsed", F.lit(RULE_VERSION))
        .withColumn("qcStatus", F.when(F.col("qcStatus").isNull(),"PASS").otherwise(F.col("qcStatus")))
        .withColumn("qcMessage", F.when(F.col("qcMessage").isNull(),"OK").otherwise(F.col("qcMessage")))
        .withColumn("issueCount", F.when(F.col("issueCount").isNull(),0).otherwise(F.col("issueCount")))
        .withColumn("createdTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))
        .select("qcRunId","ruleVersionUsed","studyId","sequence","qcStatus","qcMessage","issueCount","createdTsUtc")
    )

# ---------- write outputs ----------
qcDetailsDf.write.format("delta").mode("append").saveAsTable(qcDetailsTable)
qcSummaryDf.write.format("delta").mode("append").saveAsTable(qcSummaryTable)
qcLogDf = spark.createDataFrame(qc_log_rows)
qcLogDf.write.format("delta").mode("append").saveAsTable(qcLogTable)

print("05SilverQC complete.")
print("qcRunId:", qcRunId)
print("Details:", qcDetailsTable)
print("Summary:", qcSummaryTable)
print("Log:", qcLogTable)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 6 — Display outputs for this run

# COMMAND ----------

display(
    spark.table(qcSummaryTable)
         .filter(F.col("qcRunId") == qcRunId)
         .orderBy("qcStatus", "studyId", "sequence")
)

display(
    spark.table(qcDetailsTable)
         .filter(F.col("qcRunId") == qcRunId)
         .orderBy("studyId","sequence","sourceSheet","columnName","checkType")
)

display(
    spark.table(qcLogTable)
         .filter(F.col("qcRunId") == qcRunId)
         .orderBy(F.col("status").desc(), "silverTable")
)