# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose: Silver Transform
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC <small>
# MAGIC
# MAGIC **This notebook creates the Silver layer from Bronze after the Stoplight validation step.**
# MAGIC
# MAGIC Only studies that **pass the core data validation** are processed in this stage.  
# MAGIC Studies that fail the core data check are excluded from the pipeline.
# MAGIC
# MAGIC ### Inputs
# MAGIC - Bronze tables:  
# MAGIC   `gems_catalog.gems_schema.bronze*`
# MAGIC
# MAGIC - Stoplight validation results:  
# MAGIC   `gems_catalog.gems_schema.gemsValidationStoplight`
# MAGIC
# MAGIC - Template workbook (used to identify example rows):  
# MAGIC   `/Volumes/gems_catalog/gems_schema/blob_gems_data/DataEntryTemplate.xlsx`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What this notebook does
# MAGIC
# MAGIC 1. **Identify the latest Stoplight run** and select studies where:
# MAGIC
# MAGIC    `coreDataStatus = PASS`
# MAGIC
# MAGIC    These studies proceed to the Silver layer even if some **non-core promised datasets are missing**.
# MAGIC
# MAGIC 2. **Process each Bronze table**
# MAGIC
# MAGIC    For every Bronze dataset:
# MAGIC
# MAGIC    - Filter to studies passing the **core data validation**
# MAGIC    - Clean string values:
# MAGIC      - Trim whitespace
# MAGIC      - Normalize missing markers to empty string  
# MAGIC        `"NA"`, `"N/A"`, `"na"`, `"."`, `"null"` (case-insensitive)
# MAGIC    - Remove rows that exactly match **example rows from the template workbook**
# MAGIC    - Preserve all metadata columns added during Bronze ingest
# MAGIC    - Write cleaned data to the corresponding Silver table
# MAGIC
# MAGIC 3. **Incrementally overwrite by study**
# MAGIC
# MAGIC    For each `(studyId, sequence)`:
# MAGIC
# MAGIC    - Only that study’s partition is replaced
# MAGIC    - Previous versions remain unaffected for other studies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Outputs
# MAGIC
# MAGIC - **Silver tables**
# MAGIC
# MAGIC   `gems_catalog.gems_schema.silver<SheetName>`
# MAGIC
# MAGIC   These contain cleaned datasets ready for quality checks.
# MAGIC
# MAGIC - **Operational log**
# MAGIC
# MAGIC   `gems_catalog.gems_schema.opsSilverTransformLog`
# MAGIC
# MAGIC   Records transformation status, row counts, and errors for each study and table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notes
# MAGIC
# MAGIC - Silver focuses on **data cleaning and structural normalization** only.
# MAGIC - **Schema validation, range checks, and rulebook enforcement** occur in the next stage (**Silver QC**).
# MAGIC - Bronze retains the full ingestion history, while Silver represents the **latest cleaned version per study**.
# MAGIC
# MAGIC </small>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Imports + config

# COMMAND ----------

from datetime import datetime, timezone
import re
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

catalog = "gems_catalog"
schema  = "gems_schema"

bronzePrefix = f"{catalog}.{schema}.bronze"
stoplightTable = f"{catalog}.{schema}.gemsValidationStoplight"

# Silver table prefix requested by you
silverPrefix = f"{catalog}.{schema}.silver"

# Ops log
silverLogTable = f"{catalog}.{schema}.opsSilverTransformLog"

# Template workbook (ground truth for example rows)
dataEntryTemplatePath = f"/Volumes/{catalog}/{schema}/blob_gems_data/DataEntryTemplate.xlsx"

# Metadata columns that Bronze adds (do not use in example-row signature)
META_COLS = {
    "studyId","contractName","sequence","workbookFile","workbookPath",
    "sourceSheet","gateRunId","ingestRunId","ingestTsUtc"
}

# Missing markers -> normalize to empty string
MISSING_MARKERS = {"na", "n/a", ".", "null"}

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def clean_cell_str(x: str) -> str:
    """
    - Trim
    - Convert missing markers (NA, N/A, ., null) -> ""
    """
    if x is None:
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    if s.strip().lower() in MISSING_MARKERS:
        return ""
    return s

def list_bronze_tables() -> list:
    """
    Returns fully-qualified bronze tables in this schema that start with 'bronze'
    """
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    out = []
    for r in rows:
        t = r["tableName"]
        if t.lower().startswith("bronze"):
            out.append(f"{catalog}.{schema}.{t}")
    return sorted(out)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Get latest PASS studies from Stoplight

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
        "coreDataMessage", "promisedDataMessage"
    )
    .dropDuplicates()
)

# This is the list you use for joins/processing in later cells
passStudies = corePassStudies.select("studyId", "sequence").dropDuplicates()

print("Latest stoplight gateRunId:", latestStoplightRun)
print("Core PASS (studyId, sequence) count:", passStudies.count())
display(corePassStudies.orderBy("studyId", "sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Load template example-row signatures for ALL sheets

# COMMAND ----------

# MAGIC %md
# MAGIC ### This builds:
# MAGIC
# MAGIC template_sheet_map: normalized name -> actual sheet name in Excel
# MAGIC
# MAGIC template_cols_map: normalized sheet -> template column order
# MAGIC
# MAGIC template_example_sigs: normalized sheet -> set of row signatures

# COMMAND ----------

import openpyxl

# Load template workbook sheet names
wb = openpyxl.load_workbook(dataEntryTemplatePath, read_only=True, data_only=True)
template_sheet_names = wb.sheetnames
wb.close()

template_sheet_map = {norm(s): s for s in template_sheet_names}

def load_template_sheet_examples(sheetName: str):
    """
    Reads template sheet data rows (after skipping definition/prioritization/unit rows)
    and returns:
      - columns in order
      - set of signature strings representing example rows
    """
    try:
        pdf = pd.read_excel(
            dataEntryTemplatePath,
            sheet_name=sheetName,
            engine="openpyxl",
            skiprows=[1,2,3],     # skip Excel rows 2,3,4
            dtype=str,
            keep_default_na=False
        )
    except Exception:
        return [], set()

    pdf.columns = [str(c).strip() for c in pdf.columns]

    if pdf.shape[0] == 0:
        return list(pdf.columns), set()

    # Clean cells the same way we clean Silver
    for c in pdf.columns:
        pdf[c] = pdf[c].map(clean_cell_str)

    sigs = set(pdf.astype(str).fillna("").agg("||".join, axis=1).tolist())
    return list(pdf.columns), sigs

template_cols_map = {}
template_example_sigs = {}

for ns, sheetName in template_sheet_map.items():
    cols, sigs = load_template_sheet_examples(sheetName)
    template_cols_map[ns] = cols
    template_example_sigs[ns] = sigs

print("Template sheets loaded:", len(template_sheet_map))
print("Example signatures built for:", len(template_example_sigs))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — Transform all Bronze tables → Silver + ops log

# COMMAND ----------

# MAGIC %md
# MAGIC Only overwrites the studies in the current PASS set (per studyId + sequence)
# MAGIC
# MAGIC Leaves other studies untouched
# MAGIC
# MAGIC Writes opsSilverTransformLog in a “current-state” way (overwrite per studyId + sequence + silverTable)
# MAGIC

# COMMAND ----------

bronze_tables = list_bronze_tables()
print("Bronze tables discovered:", len(bronze_tables))
for t in bronze_tables:
    print(" -", t)

silverRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Collect PASS keys once (small list)
pass_keys = passStudies.select("studyId","sequence").dropDuplicates().collect()

def bronze_suffix(full_table: str) -> str:
    t = full_table.split(".")[-1]
    return t[len("bronze"):] if t.lower().startswith("bronze") else t

def make_signature_expr(cols_in_order):
    return F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols_in_order]
    )

def normalize_missing_markers(df):
    # Trim + normalize missing markers to ""
    missing_list = [m.lower() for m in MISSING_MARKERS]
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
            df = df.withColumn(c, F.when(F.col(c).isNull(), F.lit("")).otherwise(F.col(c)))
            df = df.withColumn(
                c,
                F.when(F.lower(F.col(c)).isin(missing_list), F.lit("")).otherwise(F.col(c))
            )
    return df

logRows = []

for bt in bronze_tables:
    suffix = bronze_suffix(bt)
    silverTable = f"{silverPrefix}{suffix}"

    try:
        bronzeDf = spark.table(bt)

        # Clean strings early (so signatures match template after cleaning)
        bronzeDf = normalize_missing_markers(bronzeDf)

        # Template matching key (normalized)
        ns = norm(suffix)
        tmpl_sigs = template_example_sigs.get(ns, set())
        tmpl_cols = template_cols_map.get(ns, [])

        for k in pass_keys:
            sid = str(k["studyId"])
            seq = str(k["sequence"])

            # Filter to one study/sequence
            df = bronzeDf.filter((F.col("studyId") == sid) & (F.col("sequence") == seq))

            if df.rdd.isEmpty():
                # No rows for this sheet for this study: leave Silver untouched, but log it
                logRows.append({
                    "silverRunId": silverRunId,
                    "studyId": sid,
                    "sequence": seq,
                    "bronzeTable": bt,
                    "silverTable": silverTable,
                    "status": "NO_ROWS_FOR_STUDY",
                    "rowCountIn": "0",
                    "rowCountOut": "0",
                    "errorMessage": "",
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })
                continue

            # Build signature columns for example-row removal
            dataCols = [c for c in tmpl_cols if (c in df.columns and c not in META_COLS)]
            if len(dataCols) == 0:
                dataCols = [c for c in df.columns if c not in META_COLS]

            sigCol = make_signature_expr(dataCols).alias("_sig")
            sigDf = df.select("*", sigCol)

            # Remove example rows (ignore any row that matches ANY template example row)
            if len(tmpl_sigs) > 0:
                sigDf = sigDf.filter(~F.col("_sig").isin(list(tmpl_sigs)))

            cleaned = sigDf.drop("_sig")

            rowCountIn = df.count()
            rowCountOut = cleaned.count()

            # IMPORTANT: incremental overwrite only this study/sequence in this Silver table
            (
                cleaned.write
                    .format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", f"studyId = '{sid}' AND sequence = '{seq}'")
                    .option("mergeSchema", "true")
                    .saveAsTable(silverTable)
            )

            logRows.append({
                "silverRunId": silverRunId,
                "studyId": sid,
                "sequence": seq,
                "bronzeTable": bt,
                "silverTable": silverTable,
                "status": "SUCCESS",
                "rowCountIn": str(rowCountIn),
                "rowCountOut": str(rowCountOut),
                "errorMessage": "",
                "createdTsUtc": datetime.now(timezone.utc).isoformat()
            })

    except Exception as e:
        # If the whole table processing fails, log one row (no studyId/sequence)
        logRows.append({
            "silverRunId": silverRunId,
            "studyId": "",
            "sequence": "",
            "bronzeTable": bt,
            "silverTable": silverTable,
            "status": "FAILED_TABLE_LEVEL",
            "rowCountIn": "",
            "rowCountOut": "",
            "errorMessage": str(e),
            "createdTsUtc": datetime.now(timezone.utc).isoformat()
        })

# ---- Write ops log as "current state" per (studyId, sequence, silverTable) ----
logSchema = StructType([
    StructField("silverRunId", StringType(), True),
    StructField("studyId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("bronzeTable", StringType(), True),
    StructField("silverTable", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rowCountIn", StringType(), True),
    StructField("rowCountOut", StringType(), True),
    StructField("errorMessage", StringType(), True),
    StructField("createdTsUtc", StringType(), True),
])

logDf = spark.createDataFrame(logRows, schema=logSchema)

# Create table if missing, otherwise overwrite per key
if not spark.catalog.tableExists(silverLogTable):
    logDf.write.format("delta").mode("overwrite").saveAsTable(silverLogTable)
else:
    # overwrite per (studyId, sequence, silverTable)
    keys = logDf.select("studyId","sequence","silverTable").dropDuplicates().collect()
    for kk in keys:
        sid = str(kk["studyId"])
        seq = str(kk["sequence"])
        st  = str(kk["silverTable"])
        part = logDf.filter((F.col("studyId")==sid) & (F.col("sequence")==seq) & (F.col("silverTable")==st))

        (part.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"studyId = '{sid}' AND sequence = '{seq}' AND silverTable = '{st}'")
            .saveAsTable(silverLogTable)
        )

display(logDf.orderBy(F.col("status").desc(), F.col("silverTable"), F.col("studyId"), F.col("sequence")))
print("04SilverTransform complete. silverRunId:", silverRunId)
print("Ops log table:", silverLogTable)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Quick sanity checks / previews

# COMMAND ----------

# 1) Show PASS studies used in this run
print("Latest stoplight gateRunId used:", latestStoplightRun)
display(passStudies.orderBy("studyId","sequence"))

# 2) Show latest Silver ops log rows for this runId
display(
    spark.table(silverLogTable)
         .filter(F.col("silverRunId") == silverRunId)
         .orderBy(F.col("status").desc(), "silverTable", "studyId", "sequence")
)

# 3) Preview one Silver table
previewSuffix = "AnimalCharacteristics"  # change
previewTable = f"{silverPrefix}{previewSuffix}"
print("Preview table:", previewTable)

if spark.catalog.tableExists(previewTable):
    display(
        spark.table(previewTable)
             .join(passStudies, on=["studyId","sequence"], how="inner")
             .orderBy("studyId","sequence")
    )
else:
    print("Table does not exist yet:", previewTable)