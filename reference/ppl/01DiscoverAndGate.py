# Databricks notebook source
# MAGIC %md
# MAGIC Purpose: Discover and Gate Study Workbooks
# MAGIC
# MAGIC This notebook performs the initial discovery and gating step of the GEMS data pipeline. It scans the study workbook storage location and identifies candidate Excel files for processing.
# MAGIC
# MAGIC For each workbook, the notebook:
# MAGIC
# MAGIC • Parses the filename to extract studyId, contractName, and sequence
# MAGIC • Computes a file hash to detect whether the workbook version has already been processed
# MAGIC • Checks whether a matching Data Use Agreement (DUA) file exists
# MAGIC • Validates that the Checklist sheet confirms the study is ready for submission
# MAGIC • Skips studies whose workbook hash matches the most recently processed version
# MAGIC
# MAGIC Only workbooks that represent new or updated study versions and pass the required gating conditions are forwarded to the ingestion stage.
# MAGIC
# MAGIC The resulting dataset is stored in gems_catalog.gems_schema.gemsGateResults, which serves as the control input for the Bronze ingest notebook.
# MAGIC
# MAGIC This step ensures that the pipeline processes only approved and changed study submissions, preventing unnecessary reprocessing and preserving compute efficiency.
# MAGIC
# MAGIC The following only needs to be done once
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gems_catalog.gems_schema.opsStudyVersions (
# MAGIC     studyId STRING,
# MAGIC     sequence STRING,
# MAGIC     fileHash STRING,
# MAGIC     processedTs TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Imports + config + list files

# COMMAND ----------

import os
import re
import hashlib
import pandas as pd
from datetime import datetime

dataVolumePath = "/Volumes/gems_catalog/gems_schema/blob_gems_data"
duaVolumePath  = "/Volumes/gems_catalog/gems_schema/blob_gems_dua"

workbookFiles = sorted([f for f in os.listdir(dataVolumePath) if f.lower().endswith(".xlsx")])
duaFiles = sorted([f for f in os.listdir(duaVolumePath) if f.lower().endswith(".pdf")])

targetWorkbookFiles = ["012_PuchunNiu_1.xlsx", "013_PuchunNiu_2.xlsx"]

print("Workbook count:", len(workbookFiles))
print("DUA count:", len(duaFiles))
print("Target workbooks found:", [f for f in workbookFiles if f in targetWorkbookFiles])
print("DUA files:", duaFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Helper functions (filename parse + checklist gate)

# COMMAND ----------

def parseWorkbookFilename(workbookFileName: str):
    """
    012_PuchunNiu_1.xlsx -> studyId=012, contractName=PuchunNiu, sequence=1
    """
    match = re.match(
        r"^(?P<studyId>\d+)_+(?P<contractName>.+?)_+(?P<sequence>\d+)\.xlsx$",
        workbookFileName,
        re.IGNORECASE
    )
    if not match:
        return None

    return match.group("studyId"), match.group("contractName"), match.group("sequence")


def isChecklistApproved(checklistDf: pd.DataFrame):
    """
    MVP rule: approve if the Checklist sheet contains either:
      - 'Completed'
      - 'Not relevant'
    anywhere in the sheet (case-insensitive).
    """
    flat = checklistDf.astype(str).fillna("").applymap(lambda x: x.strip().lower())
    textBlob = " ".join(flat.values.ravel())

    return (
        "completed" in textBlob
        or "not relevant" in textBlob
        or "not-relevant" in textBlob
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Build gateResults (DUA match + checklist ok)

# COMMAND ----------

# ---------------------------------------------
# Purpose:
#   - Loop over selected workbook files
#   - Parse studyId / contractName / sequence from filename
#   - Compute workbook hash
#   - Skip ONLY if:
#         (a) workbook hash unchanged
#         (b) study already exists in Gold
#   - Check that a matching DUA PDF exists
#   - Read the Checklist sheet
#   - Approve ONLY if Status row values are
#         "Completed" or "Not relevant"
#   - Fail closed if anything cannot be read
# ---------------------------------------------

gateRows = []

for workbookFileName in workbookFiles:

    # MVP: only process the two example workbooks
    if workbookFileName not in targetWorkbookFiles:
        continue

    # -------------------------------
    # Parse filename
    # -------------------------------
    parsed = parseWorkbookFilename(workbookFileName)

    if not parsed:
        gateRows.append({
            "studyId": None,
            "contractName": None,
            "sequence": None,
            "fileHash": None,
            "workbookFile": workbookFileName,
            "workbookPath": os.path.join(dataVolumePath, workbookFileName),
            "duaFile": None,
            "duaPath": None,
            "duaExists": False,
            "checklistOk": False,
            "checklistError": "Filename pattern not recognized"
        })
        continue

    studyId, contractName, sequence = parsed
    workbookPath = os.path.join(dataVolumePath, workbookFileName)

    # --------------------------------
    # Compute workbook hash
    # --------------------------------
    with open(workbookPath, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    # --------------------------------
    # Check previous hash
    # --------------------------------
    prev = spark.sql(f"""
    SELECT fileHash
    FROM gems_catalog.gems_schema.opsStudyVersions
    WHERE studyId='{studyId}' AND sequence='{sequence}'
    ORDER BY processedTs DESC
    LIMIT 1
    """).collect()

    skip_ingest = (len(prev) > 0 and prev[0]["fileHash"] == file_hash)

    if skip_ingest:
        print(f"Skipping unchanged study {studyId}-{sequence}")
        continue

    # -------------------------------
    # DUA matching
    # -------------------------------
    duaFile = f"{contractName}.pdf"
    duaPath = os.path.join(duaVolumePath, duaFile)
    duaExists = duaFile in duaFiles

    # -------------------------------
    # Checklist gate (fail closed)
    # -------------------------------
    checklistOk = True
    checklistError = None

    try:

        checklistDf = pd.read_excel(
            workbookPath,
            sheet_name="Checklist",
            engine="openpyxl"
        )

        normalized = (
            checklistDf.astype(str)
            .fillna("")
            .applymap(lambda x: x.strip().lower())
        )

        firstCol = normalized.iloc[:, 0]
        statusRowIdx = firstCol[firstCol == "status"].index

        if len(statusRowIdx) == 0:
            checklistOk = False
            checklistError = "Status row not found in Checklist sheet"

        else:

            idx = statusRowIdx[0]
            statusRow = normalized.loc[idx, :].tolist()

            allowedValues = {"completed", "not relevant"}

            statusValues = [v for v in statusRow[1:] if v != ""]

            if len(statusValues) == 0:
                checklistOk = False
                checklistError = "Status row contains no values"

            else:

                checklistOk = all(v in allowedValues for v in statusValues)

                if not checklistOk:
                    checklistError = (
                        "One or more Status values are not "
                        "'Completed' or 'Not relevant'"
                    )

    except Exception as e:

        checklistOk = False
        checklistError = str(e)

    # -------------------------------
    # Append gate result row
    # -------------------------------
    gateRows.append({
        "studyId": studyId,
        "contractName": contractName,
        "sequence": sequence,
        "fileHash": file_hash,
        "workbookFile": workbookFileName,
        "workbookPath": workbookPath,
        "duaFile": duaFile,
        "duaPath": duaPath,
        "duaExists": duaExists,
        "checklistOk": checklistOk,
        "checklistError": checklistError
    })

# ---------------------------------------------
# Convert to DataFrame
# ---------------------------------------------
gateDf = pd.DataFrame(gateRows)

if gateDf.empty:
    print("No studies require processing. All workbooks unchanged.")
else:
    display(gateDf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — Write gateDf to ops.gateResults (with runId)

# COMMAND ----------

# DBTITLE 1,Cell 8
# ---------------------------------------------
# Behavior:
#   - checklistError remains NULL when no error
#   - New runs append new rows
# ---------------------------------------------

from pyspark.sql import functions as F

# 1) Unique run identifier
runId = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

# 2) STOP PIPELINE if nothing to process
if gateDf.empty:

    print("========================================")
    print("Pipeline stopped")
    print("No new studies and no changes detected.")
    print("========================================")

    dbutils.notebook.exit("NO_CHANGES")

# 3) Convert pandas -> spark
sparkGateDf = (
    spark.createDataFrame(gateDf)
    .withColumn("runId", F.lit(runId))
    .withColumn("checklistError", F.col("checklistError").cast("string"))
)

# 4) Write results
(
    sparkGateDf.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("gems_catalog.gems_schema.gemsGateResults")
)

display(sparkGateDf)

print(f"Saved to gems_catalog.gems_schema.gemsGateResults with runId {runId}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Verify latest run only

# COMMAND ----------

# DBTITLE 1,Cell 10
# ---------------------------------------------

latestRunId = (
    spark.sql("""
        SELECT max(runId) AS runId
        FROM gems_catalog.gems_schema.gemsGateResults
    """).collect()[0]["runId"]
)

display(
    spark.sql(f"""
        SELECT studyId, contractName, sequence, workbookFile, workbookPath,
               duaFile, duaPath, duaExists, checklistOk, runId
        FROM gems_catalog.gems_schema.gemsGateResults
        WHERE runId = '{latestRunId}'
        ORDER BY studyId
    """)
)
