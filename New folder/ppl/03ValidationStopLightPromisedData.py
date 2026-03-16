# Databricks notebook source
# MAGIC %md
# MAGIC #### Purpose
# MAGIC
# MAGIC This notebook performs the **data completeness validation (“Stoplight”) check** after Bronze ingest.
# MAGIC
# MAGIC It ensures that each study provides the **project-required core datasets** and any **additional datasets declared in the DUA summary** before downstream transformation and quality control steps are executed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What this notebook does
# MAGIC
# MAGIC - **Reads** the DUA summary sheet from the Unity Catalog Volume.
# MAGIC - **Builds** a promised-data matrix (1/0) per study based on the DUA summary.
# MAGIC - **Maps** DUA sheet names (non-camelCase) to the corresponding Bronze sheet names (camelCase).
# MAGIC
# MAGIC For each **(studyId, sequence)** successfully ingested:
# MAGIC
# MAGIC #### 1. Core Data Validation (Project-Required)
# MAGIC
# MAGIC The notebook verifies that the following **required datasets** are present and contain real data:
# MAGIC
# MAGIC - AnimalCharacteristics  
# MAGIC - ExperimentalDesign  
# MAGIC - DietNutrientComposition  
# MAGIC - IntakePerDay  
# MAGIC - GreenFeedPelletComponents  
# MAGIC - GreenFeedDataFileReference  
# MAGIC
# MAGIC For each required dataset it checks that:
# MAGIC
# MAGIC - The corresponding **Bronze table exists**
# MAGIC - The study contains **at least one real data row** (template example rows are ignored)
# MAGIC
# MAGIC Failure in any of these datasets results in **coreDataStatus = FAIL**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Additional Promised Data Validation (DUA-Declared)
# MAGIC
# MAGIC For datasets marked as **promised (value = 1)** in the DUA summary, the notebook verifies that:
# MAGIC
# MAGIC - The corresponding **Bronze table exists**
# MAGIC - The dataset contains **at least one real data row**
# MAGIC
# MAGIC Special checks are applied where relevant:
# MAGIC
# MAGIC - **Milk composition**  
# MAGIC   At least one value must exist in the columns  
# MAGIC   `MilkProtein`, `MilkFat`, or `MilkLactose`.
# MAGIC
# MAGIC - **Nutrient digestibility**  
# MAGIC   At least one value must exist in  
# MAGIC   `DryMatterDigestibility`, `CrudeProteinDigestibility`, or the digestibility column in position K.
# MAGIC
# MAGIC Failures in these datasets result in **promisedDataStatus = FAIL**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Stoplight Status
# MAGIC
# MAGIC Each study receives three validation indicators:
# MAGIC
# MAGIC - **coreDataStatus**  
# MAGIC   PASS / FAIL for project-required datasets
# MAGIC
# MAGIC - **promisedDataStatus**  
# MAGIC   PASS / FAIL for additional DUA-promised datasets
# MAGIC
# MAGIC - **status (overall)**  
# MAGIC   FAIL if either of the above fails, otherwise PASS
# MAGIC
# MAGIC Downstream pipeline stages use **coreDataStatus** to determine whether a study proceeds to transformation and quality checks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Outputs Written
# MAGIC
# MAGIC This notebook writes three Delta tables:
# MAGIC
# MAGIC - **gemsValidationStoplight**  
# MAGIC   Stoplight validation results per study and sequence, including core and promised data checks.
# MAGIC
# MAGIC - **opsEmailQueueStoplight**  
# MAGIC   Email routing queue:  
# MAGIC   - **To:** Data Manager  
# MAGIC   - **CC:** Contributor (if different)  
# MAGIC   - **Always CC:** pn287@cornell.edu
# MAGIC
# MAGIC - **opsChecklistActions**  
# MAGIC   Records which sheets should be switched to **“In Progress”** in the checklist when required or promised data is missing.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Config

# COMMAND ----------

from datetime import datetime, timezone
import re
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)

catalog = "gems_catalog"
schema  = "gems_schema"

bronzePrefix = f"{catalog}.{schema}.bronze"
bronzeIngestLogTable = f"{catalog}.{schema}.gemsbronzeingestlog"

gateResultsTable      = f"{catalog}.{schema}.gemsValidationStoplight"
emailQueueTable       = f"{catalog}.{schema}.opsEmailQueueStoplight"
checklistActionsTable = f"{catalog}.{schema}.opsChecklistActions"

# Where the DUA summary workbook lives (in your UC Volume)
dua_summary_path = f"/Volumes/{catalog}/{schema}/blob_gems_data/DUA summary.xlsx"
dua_sheet_name   = "DUASummary"

# MVP sheets we validate as "promised sheet present + has rows"
mvpSheets = [
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
]

# There must be data in the following sheets as required by the project
CORE_SHEETS = [
    "AnimalCharacteristics",
    "ExperimentalDesign",
    "DietNutrientComposition",
    "IntakePerDay",
    "GreenFeedPelletComponents",
    "GreenFeedDataFileReference",
]

# Special promises (these are NOT sheets in DUA but flags)
# We'll locate these columns in DUA summary via normalization.
SPECIAL_MILK_COMPOSITION_FLAG = "Milk composition (if applicable)"
SPECIAL_NUTR_DIGEST_FLAG      = "Nutrient digestibility"

# Milk composition columns (must ALL have some data)
MILK_COMPOSITION_COLS = ["MilkProtein", "MilkFat", "MilkLactose"]

# Digestibility columns:
DIG_DM_COL = "DryMatterDigestibility"     # column G
DIG_CP_COL = "CrudeProteinDigestibility"  # column I
# Column K: variable name, we will locate by position K (index 10, 0-based)

def bronzeTable(sheet: str) -> str:
    return f"{bronzePrefix}{sheet}"

def table_exists(fullname: str) -> bool:
    return spark.catalog.tableExists(fullname)

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def to3(s):
    # Ensure studyId looks like "012"
    s = str(s).strip()
    s = re.sub(r"\.0$", "", s)
    if s.isdigit():
        return s.zfill(3)
    return s

def sanitize_email(s) -> str:
    """Remove null bytes, NBSP, control chars from email (Excel/DUA corruption)."""
    if s is None:
        return ""
    s = str(s)
    s = "".join(c for c in s if ord(c) >= 32 or c in "\n\r\t")
    s = s.replace("\xa0", " ").replace("\u00A0", " ")
    return s.strip()

def nonempty_count(df, colname: str) -> int:
    # Works for your Bronze (strings); treats "" as empty
    return (
        df.filter(~(F.col(colname).isNull() | (F.trim(F.col(colname)) == "")))
          .count()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Load DUA summary workbook (pandas)

# COMMAND ----------

dua_pdf = pd.read_excel(dua_summary_path, sheet_name=dua_sheet_name, engine="openpyxl", dtype=str, keep_default_na=False)

# Strip column names
dua_pdf.columns = [str(c).strip() for c in dua_pdf.columns]

# Identify key columns (by normalized name)
colmap = {norm(c): c for c in dua_pdf.columns}

def pick_col(*candidates):
    for cand in candidates:
        for k, v in colmap.items():
            if k == norm(cand):
                return v
    return None

study_col = pick_col("Study ID", "StudyId", "study_id")
contributor_col = pick_col("Contributor", "contributor")
contrib_email_col = pick_col("Contributor email", "Contributor Email", "contributor_email")
dm_name_col = pick_col("Contact person (data manager)", "Data manager", "Contact person", "contact_person")
dm_email_col = pick_col("Contact email (data manager)", "Data manager email", "Contact email", "contact_email")

# Optional descriptive fields
n_animals_col = pick_col("Number of animals", "No. animals", "n_animals")
study_len_col = pick_col("Study length (Days)", "Study length", "length_days")

# Special flags columns in DUA (not camelCase)
# We'll locate them by "contains" on normalized column name
milk_flag_col = None
dig_flag_col = None
for c in dua_pdf.columns:
    nc = norm(c)
    if milk_flag_col is None and norm(SPECIAL_MILK_COMPOSITION_FLAG) in nc:
        milk_flag_col = c
    if dig_flag_col is None and norm(SPECIAL_NUTR_DIGEST_FLAG) in nc:
        dig_flag_col = c

assert study_col is not None, "Could not find Study ID column in DUA summary sheet."

# Clean + standardize studyId
dua_pdf["studyId"] = dua_pdf[study_col].apply(to3)

display(spark.createDataFrame(dua_pdf.head(10)))
print("DUA loaded. Rows:", len(dua_pdf), "Columns:", len(dua_pdf.columns))
print("study_col:", study_col)
print("milk_flag_col:", milk_flag_col)
print("dig_flag_col:", dig_flag_col)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Build “promise map” for sheets (DUA 1/0 → Bronze sheets)

# COMMAND ----------

# Build a mapping: bronzeSheetName -> duaColumnName (if present)
dua_cols_norm = {norm(c): c for c in dua_pdf.columns}

sheet_promise_map = {}
for sheet in mvpSheets:
    # match by normalized sheet name (e.g., BodyWeight -> bodyweight)
    key = norm(sheet)
    if key in dua_cols_norm:
        sheet_promise_map[sheet] = dua_cols_norm[key]
    else:
        # try relaxed matching for DUA with spaces (e.g., "Body weight")
        # any DUA col whose normalized string equals the sheet normalized string
        found = None
        for c in dua_pdf.columns:
            if norm(c) == key:
                found = c
                break
        if found:
            sheet_promise_map[sheet] = found

print("Sheet promise map (bronzeSheet -> DUA column):")
for k, v in sheet_promise_map.items():
    print("  ", k, "->", v)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — template ground truth

# COMMAND ----------

# ---------------------------------------------
# Template example rows (ground truth)
# Insert AFTER Cell 3
# ---------------------------------------------

dataEntryTemplatePath = f"/Volumes/{catalog}/{schema}/blob_gems_data/DataEntryTemplate.xlsx"

def load_template_examples(sheetName: str) -> set:
    """
    Returns set of row signatures representing example rows for a sheet.
    Any user row matching one of these signatures is treated as "example" and ignored.
    """
    try:
        pdf = pd.read_excel(
            dataEntryTemplatePath,
            sheet_name=sheetName,
            engine="openpyxl",
            skiprows=[1, 2, 3],   # same as Bronze ingest
            dtype=str,
            keep_default_na=False
        )
    except Exception:
        return set()

    pdf.columns = [str(c).strip() for c in pdf.columns]
    if pdf.shape[0] == 0:
        return set()

    return set(pdf.astype(str).fillna("").agg("||".join, axis=1).tolist())

template_example_sigs = {s: load_template_examples(s) for s in mvpSheets}

print("Loaded template example signatures:")
for s in mvpSheets:
    print(f"  {s}: {len(template_example_sigs[s])} example rows")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Identify candidates (which studyId/sequence to validate)

# COMMAND ----------

ingestLog = spark.table(bronzeIngestLogTable)

latest = (
    ingestLog
    .filter(F.col("status") == "SUCCESS")
    .groupBy("studyId", "sequence")
    .agg(F.max("ingestRunId").alias("ingestRunId"))
)

candidates = (
    ingestLog.alias("l")
    .join(latest.alias("r"), on=["studyId","sequence","ingestRunId"], how="inner")
    .select("studyId","sequence","contractName","workbookFile","workbookPath","ingestRunId")
    .dropDuplicates()
)

display(candidates.orderBy("studyId","sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 6 — Stoplight validation (promised vs delivered & coreData vs delivered)

# COMMAND ----------

# ---------------------------------------------
#   - FAIL if promised=1 but:
#       * bronze table missing OR
#       * only template example rows exist (no real rows)
#   - Special checks:
#       * Milk composition: MilkProtein/MilkFat/MilkLactose all have non-empty
#       * Nutrient digestibility: any of DM/CP/K has non-empty
# Outputs:
#   - gateResultsTable
#   - emailQueueTable
#   - checklistActionsTable
# ---------------------------------------------

gateRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def is_yes(x):
    return str(x).strip() == "1"

# Prepare DUA lookup dict by studyId
dua_by_study = {row["studyId"]: row for _, row in dua_pdf.iterrows()}

results_rows = []
email_rows   = []
action_rows  = []

cand = candidates.collect()

# Metadata columns added by Bronze ingest (exclude these from row signature)
META_COLS = {
    "studyId","contractName","sequence","workbookFile","workbookPath",
    "sourceSheet","gateRunId","ingestRunId","ingestTsUtc"
}

def count_real_rows_excluding_examples(sheet: str, df) -> int:
    """
    Count rows in df that do NOT match any example row signature
    from DataEntryTemplate for that sheet.
    """
    dataCols = [c for c in df.columns if c not in META_COLS]

    if len(dataCols) == 0:
        return 0

    tmpl = template_example_sigs.get(sheet, set())

    sigCol = F.concat_ws(
        "||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in dataCols]
    ).alias("_sig")

    sigDf = df.select(sigCol)

    # Ignore any rows that match ANY template example signature
    if len(tmpl) > 0:
        return sigDf.filter(~F.col("_sig").isin(list(tmpl))).count()
    else:
        # If template has no example rows for this sheet, any row counts as real
        return df.count()

def validate_sheet_has_real_rows(studyId: str, sequence: str, sheet: str):
    """
    Shared rule:
      - FAIL if table missing OR real rows (excluding template examples) == 0
    Returns: (ok: bool, reason: str)
    """
    t = bronzeTable(sheet)
    if not table_exists(t):
        return False, "bronze table missing"

    df = spark.table(t).filter((F.col("studyId") == studyId) & (F.col("sequence") == sequence))
    n_real = count_real_rows_excluding_examples(sheet, df)
    if n_real == 0:
        return False, "only template example rows (no real data)"
    return True, "OK"

for c in cand:
    studyId   = str(c["studyId"])
    sequence  = str(c["sequence"])
    contract  = c["contractName"]
    wbFile    = c["workbookFile"]
    wbPath    = c["workbookPath"]
    ingestRun = c["ingestRunId"]

    dua_row = dua_by_study.get(studyId, None)

    # Overall status
    status = "PASS"

    # Split validations
    coreDataStatus     = "PASS"
    promisedDataStatus = "PASS"
    coreMsgs = []
    promisedMsgs = []

    # Email routing fields from DUA
    dm_email = ""
    contrib_email = ""
    dm_name = ""
    contributor = ""
    n_animals = ""
    study_len = ""

    if dua_row is None:
        status = "FAIL"
        coreDataStatus = "FAIL"
        promisedDataStatus = "FAIL"
        coreMsgs.append("DUA summary: missing row for this studyId")
    else:
        if contributor_col: contributor = str(dua_row.get(contributor_col, "")).strip()
        if dm_name_col: dm_name = str(dua_row.get(dm_name_col, "")).strip()
        if dm_email_col: dm_email = str(dua_row.get(dm_email_col, "")).strip()
        if contrib_email_col: contrib_email = str(dua_row.get(contrib_email_col, "")).strip()
        if dm_email_col: dm_email = sanitize_email(dua_row.get(dm_email_col, ""))
        if contrib_email_col: contrib_email = sanitize_email(dua_row.get(contrib_email_col, ""))
        if n_animals_col: n_animals = str(dua_row.get(n_animals_col, "")).strip()
        if study_len_col: study_len = str(dua_row.get(study_len_col, "")).strip()

    # ---------- Core data required checks (always enforced) ----------
    if dua_row is not None:
        for sheet in CORE_SHEETS:
            ok, reason = validate_sheet_has_real_rows(studyId, sequence, sheet)
            if not ok:
                coreDataStatus = "FAIL"
                coreMsgs.append(f"{sheet}: {reason}")
                action_rows.append({
                    "gateRunId": gateRunId,
                    "studyId": studyId,
                    "sequence": sequence,
                    "sheetName": sheet,
                    "action": "set_checklist_in_progress",
                    "reason": f"core required but {reason}",
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })

    # ---------- Other promised sheet checks ----------
    if dua_row is not None:
        for sheet in mvpSheets:
            if sheet in CORE_SHEETS:
                continue  # already checked as core

            dua_col = sheet_promise_map.get(sheet, None)

            # If DUA doesn't have a column for this sheet, we do NOT enforce it
            if dua_col is None:
                continue

            promised = is_yes(dua_row.get(dua_col, "0"))
            if not promised:
                continue

            ok, reason = validate_sheet_has_real_rows(studyId, sequence, sheet)
            if not ok:
                promisedDataStatus = "FAIL"
                promisedMsgs.append(f"{sheet}: {reason}")
                action_rows.append({
                    "gateRunId": gateRunId,
                    "studyId": studyId,
                    "sequence": sequence,
                    "sheetName": sheet,
                    "action": "set_checklist_in_progress",
                    "reason": f"promised=1 but {reason}",
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })

    # ---------- Special: Milk composition (ALL THREE cols must have data) ----------
    if dua_row is not None and milk_flag_col is not None and is_yes(dua_row.get(milk_flag_col, "0")):
        t = bronzeTable("Milk")
        if not table_exists(t):
            promisedDataStatus = "FAIL"
            promisedMsgs.append("Milk composition: promised=1 but bronzeMilk missing")
            action_rows.append({
                "gateRunId": gateRunId,
                "studyId": studyId,
                "sequence": sequence,
                "sheetName": "Milk",
                "action": "set_checklist_in_progress",
                "reason": "Milk composition promised=1 but bronzeMilk missing",
                "createdTsUtc": datetime.now(timezone.utc).isoformat()
            })
        else:
            mdf = spark.table(t).filter((F.col("studyId") == studyId) & (F.col("sequence") == sequence))

            missing_cols = [cc for cc in MILK_COMPOSITION_COLS if cc not in mdf.columns]
            if missing_cols:
                promisedDataStatus = "FAIL"
                promisedMsgs.append(f"Milk composition: missing columns {missing_cols}")
            else:
                for colname in MILK_COMPOSITION_COLS:
                    cnt = nonempty_count(mdf, colname)
                    if cnt == 0:
                        promisedDataStatus = "FAIL"
                        promisedMsgs.append(f"Milk composition: {colname} has no data (requires data in MilkProtein/MilkFat/MilkLactose)")

                if any("Milk composition" in m for m in promisedMsgs):
                    action_rows.append({
                        "gateRunId": gateRunId,
                        "studyId": studyId,
                        "sequence": sequence,
                        "sheetName": "Milk",
                        "action": "set_checklist_in_progress",
                        "reason": "Milk composition promised but missing in one or more of MilkProtein/MilkFat/MilkLactose",
                        "createdTsUtc": datetime.now(timezone.utc).isoformat()
                    })

    # ---------- Special: Nutrient digestibility (ANY of G/I/K ok) ----------
    if dua_row is not None and dig_flag_col is not None and is_yes(dua_row.get(dig_flag_col, "0")):
        t = bronzeTable("Digestibility")
        if not table_exists(t):
            promisedDataStatus = "FAIL"
            promisedMsgs.append("Nutrient digestibility: promised=1 but bronzeDigestibility missing")
            action_rows.append({
                "gateRunId": gateRunId,
                "studyId": studyId,
                "sequence": sequence,
                "sheetName": "Digestibility",
                "action": "set_checklist_in_progress",
                "reason": "Nutrient digestibility promised=1 but bronzeDigestibility missing",
                "createdTsUtc": datetime.now(timezone.utc).isoformat()
            })
        else:
            ddf = spark.table(t).filter((F.col("studyId") == studyId) & (F.col("sequence") == sequence))

            # Column K by position (index 10)
            k_col = None
            if len(ddf.columns) >= 11:
                k_col = ddf.columns[10]
            else:
                cand_cols = [cc for cc in ddf.columns if "digest" in norm(cc)]
                cand_cols = [cc for cc in cand_cols if cc not in [DIG_DM_COL, DIG_CP_COL]]
                k_col = cand_cols[0] if cand_cols else None

            dm_ok = (DIG_DM_COL in ddf.columns) and (nonempty_count(ddf, DIG_DM_COL) > 0)
            cp_ok = (DIG_CP_COL in ddf.columns) and (nonempty_count(ddf, DIG_CP_COL) > 0)
            k_ok  = (k_col is not None) and (k_col in ddf.columns) and (nonempty_count(ddf, k_col) > 0)

            if not (dm_ok or cp_ok or k_ok):
                promisedDataStatus = "FAIL"
                promisedMsgs.append(f"Nutrient digestibility: promised=1 but no data in {DIG_DM_COL} OR {DIG_CP_COL} OR column-K")
                action_rows.append({
                    "gateRunId": gateRunId,
                    "studyId": studyId,
                    "sequence": sequence,
                    "sheetName": "Digestibility",
                    "action": "set_checklist_in_progress",
                    "reason": "Nutrient digestibility promised but missing in DM/CP/K",
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })

    # ---------- Overall status + messages ----------
    coreDataMessage = "OK" if not coreMsgs else " | ".join(coreMsgs)
    promisedDataMessage = "OK" if not promisedMsgs else " | ".join(promisedMsgs)

    if dua_row is None:
        status = "FAIL"
    else:
        status = "FAIL" if (coreDataStatus == "FAIL" or promisedDataStatus == "FAIL") else "PASS"

    combined = []
    if coreMsgs:
        combined.append("Core: " + " | ".join(coreMsgs))
    if promisedMsgs:
        combined.append("Promised: " + " | ".join(promisedMsgs))
    message = " || ".join(combined) if combined else "OK"

    # ---------- Gate result row ----------
    results_rows.append({
        "gateRunId": gateRunId,
        "studyId": studyId,
        "sequence": sequence,
        "contractName": contract,
        "workbookFile": wbFile,
        "workbookPath": wbPath,
        "ingestRunId": ingestRun,

        "coreDataStatus": coreDataStatus,
        "coreDataMessage": coreDataMessage,
        "promisedDataStatus": promisedDataStatus,
        "promisedDataMessage": promisedDataMessage,

        "status": status,
        "message": message,
        "createdTsUtc": datetime.now(timezone.utc).isoformat()
    })

    # ---------- Email queue row ----------
    cc_list = ["pn287@cornell.edu"]
    if contrib_email and dm_email and contrib_email.strip().lower() != dm_email.strip().lower():
        cc_list.append(contrib_email)
    elif contrib_email and not dm_email:
        cc_list.append(contrib_email)

    email_rows.append({
        "gateRunId": gateRunId,
        "studyId": studyId,
        "sequence": sequence,
        "toEmail": dm_email,
        "ccEmails": ",".join([x for x in cc_list if x]),
        "dataManagerName": dm_name,
        "contributorName": contributor,
        "contributorEmail": contrib_email,

        "coreDataStatus": coreDataStatus,
        "coreDataMessage": coreDataMessage,
        "promisedDataStatus": promisedDataStatus,
        "promisedDataMessage": promisedDataMessage,

        "status": status,
        "message": message,
        "workbookFile": wbFile,
        "workbookPath": wbPath,
        "createdTsUtc": datetime.now(timezone.utc).isoformat()
    })

# ----- Write outputs with stable STRING schema to avoid Delta merge issues -----

gateSchema = StructType([
    StructField("gateRunId", StringType(), True),
    StructField("studyId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("contractName", StringType(), True),
    StructField("workbookFile", StringType(), True),
    StructField("workbookPath", StringType(), True),
    StructField("ingestRunId", StringType(), True),

    StructField("coreDataStatus", StringType(), True),
    StructField("coreDataMessage", StringType(), True),
    StructField("promisedDataStatus", StringType(), True),
    StructField("promisedDataMessage", StringType(), True),

    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("createdTsUtc", StringType(), True),
])

emailSchema = StructType([
    StructField("gateRunId", StringType(), True),
    StructField("studyId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("toEmail", StringType(), True),
    StructField("ccEmails", StringType(), True),
    StructField("dataManagerName", StringType(), True),
    StructField("contributorName", StringType(), True),
    StructField("contributorEmail", StringType(), True),

    StructField("coreDataStatus", StringType(), True),
    StructField("coreDataMessage", StringType(), True),
    StructField("promisedDataStatus", StringType(), True),
    StructField("promisedDataMessage", StringType(), True),

    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("workbookFile", StringType(), True),
    StructField("workbookPath", StringType(), True),
    StructField("createdTsUtc", StringType(), True),
])

actionSchema = StructType([
    StructField("gateRunId", StringType(), True),
    StructField("studyId", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("sheetName", StringType(), True),
    StructField("action", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("createdTsUtc", StringType(), True),
])

gateDf   = spark.createDataFrame(results_rows, schema=gateSchema)
emailDf  = spark.createDataFrame(email_rows,   schema=emailSchema)
actionDf = spark.createDataFrame(action_rows,  schema=actionSchema)

gateDf.write.format("delta").mode("append").saveAsTable(gateResultsTable)
emailDf.write.format("delta").mode("append").saveAsTable(emailQueueTable)
actionDf.write.format("delta").mode("append").saveAsTable(checklistActionsTable)

display(gateDf.orderBy("studyId","sequence"))
display(emailDf.orderBy("studyId","sequence"))
display(actionDf.orderBy("studyId","sequence","sheetName"))

print("03ValidationStoplight complete. gateRunId:", gateRunId)