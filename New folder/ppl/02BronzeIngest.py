# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose: Bronze Ingest
# MAGIC
# MAGIC This notebook ingests approved study workbooks into the Bronze layer as raw, traceable Delta tables.
# MAGIC
# MAGIC Only workbooks listed in gems_catalog.gems_schema.gemsGateResults from the most recent gate run (runId) with duaExists = true and checklistOk = true are processed. Each workbook represents a new or updated study version, as determined during the gating step using file-hash comparison with opsStudyVersions.
# MAGIC
# MAGIC For every approved workbook, selected sheets are read directly from the Excel file and written to Bronze tables without modification. Template metadata rows (definitions, prioritization, and units) are handled separately: the unit row is captured and stored in opsSheetUnits for reference and validation in later pipeline stages.
# MAGIC
# MAGIC Standard metadata columns are appended to every Bronze table, including:
# MAGIC
# MAGIC studyId
# MAGIC contractName
# MAGIC sequence
# MAGIC workbookFile
# MAGIC workbookPath
# MAGIC sourceSheet
# MAGIC gateRunId
# MAGIC ingestRunId
# MAGIC ingestTsUtc
# MAGIC
# MAGIC These fields provide traceability, lineage, and reproducibility for all ingested records.
# MAGIC
# MAGIC No cleaning, deduplication, or filtering occurs at this stage. The Bronze layer preserves the submitted study data exactly as provided while organizing it into structured Delta tables. All validation, template-row filtering, and quality control checks are intentionally deferred to the Silver layer of the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Imports + config

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - Set paths and table names
#   - Keep all naming camelCase (per your preference)
# Notes:
#   - Bronze tables are append-only
#   - We store raw sheets with minimal typing changes
# ---------------------------------------------

import os
import re
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import functions as F

dataVolumePath = "/Volumes/gems_catalog/gems_schema/blob_gems_data"

# Source gate results table (already created by DiscoverAndGate)
gateResultsTable = "gems_catalog.gems_schema.gemsGateResults"

# Bronze table name prefix (one table per sheet, shared across all studies)
bronzeTablePrefix = "gems_catalog.gems_schema.bronze"

# Ops log table for ingest status (optional but strongly recommended)
bronzeIngestLogTable = "gems_catalog.gems_schema.gemsBronzeIngestLog"

catalog = "gems_catalog"
schema  = "gems_schema"


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Get latest approved workbooks from GateResults

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - Find latest gateRunId
#   - Select only rows where:
#       duaExists = true AND checklistOk = true
# Notes:
#   - This is how we ensure Bronze only ingests "approved" inputs
# ---------------------------------------------

latestGateRunId = (
    spark.sql(f"SELECT max(runId) AS runId FROM {gateResultsTable}")
         .collect()[0]["runId"]
)

approvedDf = spark.sql(f"""
    SELECT
      studyId,
      contractName,
      sequence,
      workbookFile,
      workbookPath,
      duaFile,
      duaPath,
      duaExists,
      checklistOk,
      checklistError,
      fileHash,
      runId AS gateRunId
    FROM {gateResultsTable}
    WHERE runId = '{latestGateRunId}'
      AND duaExists = true
      AND checklistOk = true
""")

display(approvedDf)
print("latestGateRunId:", latestGateRunId)
print("approved workbooks:", approvedDf.count())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Define which sheets to ingest + helper function

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - Define which sheets we ingest in this MVP
#   - Provide a robust function that:
#       * reads a sheet via pandas/openpyxl
#       * converts to Spark
#       * adds metadata columns
#       * writes to the correct Bronze table
# Notes:
#   - We ingest only a few sheets first to reduce risk and debugging time.
#   - Expanding to "all sheets" is easy: just add names to sheetNamesToIngest.
# ---------------------------------------------

from datetime import datetime, timezone  # ensure available (used for unit capture timestamps)

# MVP sheet list (start small; we can expand safely)
sheetNamesToIngest = [
    "Contributor",
    "AnimalCharacteristics",
    "ExperimentalDesign",
    "FeedComponents",
    "DietNutrientComposition",
    "IntakePerDay",
    "IntakeIntraday",
    "Milk",
    "BodyWeight",
    "Digestibility",
    "GreenFeedSettings",
    "GreenFeedCalibration",
    "GreenFeedPelletComponents",
    "GreenFeedDataFileReference",
    "RespirationChamberSettings",
    "RespirationChamberMeasurement",
    "SF6Settings",
    "SF6Measurement",
    # add more later (GreenFeedSettings, SF6, etc.)
]

def bronzeTableNameForSheet(sheetName: str) -> str:
    # Example: bronze + "AnimalCharacteristics" -> gems_catalog.gems_schema.bronzeAnimalCharacteristics
    return f"{bronzeTablePrefix}{sheetName}"

# NEW: ops table to store units (one row per column)
# Use your existing catalog/schema variables if you have them; otherwise set the full name string.
try:
    opsSheetUnitsTable = f"{catalog}.{schema}.opsSheetUnits"
except NameError:
    opsSheetUnitsTable = "gems_catalog.gems_schema.opsSheetUnits"


def ingestOneSheetToBronze(
    workbookPath: str,
    workbookFile: str,
    studyId: str,
    contractName: str,
    sequence: str,
    gateRunId: str,
    ingestRunId: str,
    sheetName: str
):
    # ---------------------------------------------------------
    # NEW: Capture Unit row (Excel row 4) into opsSheetUnitsTable
    # Template structure assumed consistent across all sheets:
    #   Row 1: column names
    #   Row 2: definitions
    #   Row 3: prioritization
    #   Row 4: unit
    #   Row 5+: data
    #
    # With header=0 and nrows=4:
    #   head4.iloc[0] => definitions (Excel row 2)
    #   head4.iloc[1] => prioritization (Excel row 3)
    #   head4.iloc[2] => unit (Excel row 4)   <-- we capture this
    #   head4.iloc[3] => first data row (Excel row 5) (ignored here)
    # ---------------------------------------------------------
    head4 = pd.read_excel(
        workbookPath,
        sheet_name=sheetName,
        engine="openpyxl",
        header=0,
        nrows=4,
        dtype=str,
        keep_default_na=False
    )
    head4.columns = [str(c).strip() for c in head4.columns]

    unit_dict = head4.iloc[2].to_dict()

    unit_rows = [{
        "studyId": studyId,
        "contractName": contractName,
        "sequence": sequence,
        "workbookFile": workbookFile,
        "workbookPath": workbookPath,
        "sourceSheet": sheetName,
        "columnName": str(col),
        "unit": None if pd.isna(u) else str(u),
        "gateRunId": gateRunId,
        "ingestRunId": ingestRunId,
        "ingestTsUtc": datetime.now(timezone.utc).isoformat()
    } for col, u in unit_dict.items()]

    unitsSdf = spark.createDataFrame(pd.DataFrame(unit_rows))

    (
        unitsSdf.write
            .format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"studyId = '{studyId}' AND sequence = '{sequence}' AND sourceSheet = '{sheetName}'"
            )
            .option("mergeSchema", "true")
            .saveAsTable(opsSheetUnitsTable)
    )

    # ---------------------------------------------------------
    # Existing: Read data rows only (skip template rows 2–4)
    # ---------------------------------------------------------
    pdf = pd.read_excel(
        workbookPath,
        sheet_name=sheetName,
        engine="openpyxl",
        skiprows=[1,2,3],
        dtype=str,
        keep_default_na=False
    )

    pdf.columns = [str(c).strip() for c in pdf.columns]
    
    # Convert to Spark
    sdf = spark.createDataFrame(pdf)

    # Add metadata for traceability and avoiding conflicts across studies
    sdf = (
        sdf
        .withColumn("studyId", F.lit(studyId))
        .withColumn("contractName", F.lit(contractName))
        .withColumn("sequence", F.lit(sequence))
        .withColumn("workbookFile", F.lit(workbookFile))
        .withColumn("workbookPath", F.lit(workbookPath))
        .withColumn("sourceSheet", F.lit(sheetName))
        .withColumn("gateRunId", F.lit(gateRunId))
        .withColumn("ingestRunId", F.lit(ingestRunId))
        .withColumn("ingestTsUtc", F.current_timestamp())
    )

    # Write to Bronze (overwrite this study+sheet on rerun)
    targetTable = bronzeTableNameForSheet(sheetName)

    (
        sdf.write
            .format("delta")
            .mode("overwrite")
            .option(
                "replaceWhere",
                f"studyId = '{studyId}' AND sequence = '{sequence}' AND sourceSheet = '{sheetName}'"
            )
            .option("mergeSchema", "true")
            .saveAsTable(targetTable)
    )

    totalRows = sdf.count()
    reportedRows = totalRows  # since we skipped template rows already

    return targetTable, reportedRows

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — Ingest loop + log successes/failures

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - For each approved workbook:
#       ingest each selected sheet into its Bronze table
#   - Write an ops log row per (workbook, sheet) so we can debug later
# Notes:
#   - If a sheet is missing, we log an error but continue with other sheets
# ---------------------------------------------

ingestRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

approvedRows = approvedDf.collect()

logRows = []

for r in approvedRows:
    studyId = r["studyId"]
    contractName = r["contractName"]
    sequence = r["sequence"]
    workbookFile = r["workbookFile"]
    workbookPath = r["workbookPath"]
    gateRunId = r["gateRunId"]
    file_hash = r["fileHash"]

    for sheetName in sheetNamesToIngest:
        try:
            targetTable, rowCount = ingestOneSheetToBronze(
                workbookPath=workbookPath,
                workbookFile=workbookFile,
                studyId=studyId,
                contractName=contractName,
                sequence=sequence,
                gateRunId=gateRunId,
                ingestRunId=ingestRunId,
                sheetName=sheetName
            )

            logRows.append({
                "ingestRunId": ingestRunId,
                "gateRunId": gateRunId,
                "studyId": studyId,
                "contractName": contractName,
                "sequence": sequence,
                "workbookFile": workbookFile,
                "workbookPath": workbookPath,
                "sourceSheet": sheetName,
                "targetTable": targetTable,
                "status": "SUCCESS",
                "rowCount": int(rowCount),
                "errorMessage": None,
                "ingestTsUtc": datetime.now(timezone.utc).isoformat()
            })

        except Exception as e:
            logRows.append({
                "ingestRunId": ingestRunId,
                "gateRunId": gateRunId,
                "studyId": studyId,
                "contractName": contractName,
                "sequence": sequence,
                "workbookFile": workbookFile,
                "workbookPath": workbookPath,
                "sourceSheet": sheetName,
                "targetTable": bronzeTableNameForSheet(sheetName),
                "status": "FAILED",
                "rowCount": None,
                "errorMessage": str(e),
                "ingestTsUtc": datetime.now(timezone.utc).isoformat()
            })

    spark.sql(f"""
    INSERT INTO gems_catalog.gems_schema.opsStudyVersions
    VALUES (
        '{studyId}',
        '{sequence}',
        '{file_hash}',
        current_timestamp()
    )
    """)    
    
logPdf = pd.DataFrame(logRows)
logSdf = spark.createDataFrame(logPdf)

# Force stable types (prevents Delta merge conflicts)
logSdf = spark.createDataFrame(pd.DataFrame(logRows))

(
    logSdf.write
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(bronzeIngestLogTable)
)

display(logSdf.orderBy(F.col("status").desc(), F.col("sourceSheet")))
print("Bronze ingest complete. ingestRunId:", ingestRunId)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Quick verification (show what got ingested this run)

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - Show ingest log rows for this run
#   - Preview captured units (ops table)
#   - Preview one Bronze table (data rows only)
# Notes:
#   - The Catalog UI "Sample Data" is only a preview; use SQL for full results
# ---------------------------------------------

# Make sure this matches what you set in Cell 3
try:
    opsSheetUnitsTable = f"{catalog}.{schema}.opsSheetUnits"
except NameError:
    opsSheetUnitsTable = "gems_catalog.gems_schema.opsSheetUnits"

previewSheet = "Milk"  # change to "BodyWeight", etc. if you want

# 1) Ingest log rows for this run
display(
    spark.sql(f"""
        SELECT ingestRunId, gateRunId, studyId, contractName, sequence,
               workbookFile, workbookPath, sourceSheet, targetTable,
               status, rowCount, ingestTsUtc
        FROM {bronzeIngestLogTable}
        WHERE ingestRunId = '{ingestRunId}'
        ORDER BY status DESC, sourceSheet
    """)
)

# 2) Units captured for this run (for the chosen sheet)
display(
    spark.sql(f"""
        SELECT studyId, contractName, sequence, workbookFile, sourceSheet,
               columnName, unit, ingestRunId, ingestTsUtc
        FROM {opsSheetUnitsTable}
        WHERE ingestRunId = '{ingestRunId}'
          AND sourceSheet = '{previewSheet}'
        ORDER BY studyId, sequence, columnName
    """)
)

# 3) Preview one bronze table (data rows only)
display(
    spark.sql(f"""
        SELECT *
        FROM {bronzeTableNameForSheet(previewSheet)}
        WHERE ingestRunId = '{ingestRunId}'
        ORDER BY studyId, sequence
    """)
)
