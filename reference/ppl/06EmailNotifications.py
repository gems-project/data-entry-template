# Databricks notebook source
# MAGIC %md
# MAGIC Purpose
# MAGIC
# MAGIC This notebook consolidates the results of the validation stoplight checks and quality control (QC) evaluation to determine the final processing status of each study in the GEMS data pipeline. The goal is to translate validation and QC outcomes into operational actions, including contributor notifications, checklist updates, and identification of studies eligible for promotion to the Gold layer.
# MAGIC
# MAGIC While earlier pipeline steps verify whether the required datasets were delivered according to the Data Use Agreement (Stoplight validation), this notebook evaluates the combined validation and QC status and prepares the corresponding operational responses for each study.
# MAGIC
# MAGIC Main Tasks
# MAGIC
# MAGIC 1. Integrate validation and QC results
# MAGIC
# MAGIC The notebook loads the most recent validation results from gemsValidationStoplight and QC results from silverQcSummary. These outputs are merged to create a unified status table describing whether each study passes the core data requirements, promised data checks, and QC validation.
# MAGIC
# MAGIC 2. Prepare email notifications
# MAGIC
# MAGIC Based on the validation and QC outcomes, the notebook generates standardized notification messages describing the study status and any detected issues. These messages include the study identifier, workbook reference, and detailed explanations of missing core data, missing promised datasets, or QC violations. The messages are written to the operational email queue opsEmailQueueQc, which will later be processed by the email-sending notebook.
# MAGIC
# MAGIC 3. Generate checklist actions
# MAGIC
# MAGIC When validation or QC issues are detected, the notebook identifies the affected sheets and writes corresponding actions to opsChecklistActions. These actions automatically set the relevant checklist items to “In Progress”, helping contributors identify which sections of the workbook require correction before resubmission.
# MAGIC
# MAGIC 4. Identify Gold-layer candidates
# MAGIC
# MAGIC Studies that pass the core data validation, promised data validation, and QC checks are flagged as eligible for promotion to the Gold layer of the GEMS data pipeline. These studies are recorded in opsGoldCandidates for downstream processing and publication-ready data integration.
# MAGIC
# MAGIC 5. Output and Next Step
# MAGIC
# MAGIC The outputs of this notebook are written to three operational tables that support the final stages of the pipeline.
# MAGIC
# MAGIC opsEmailQueueQc  
# MAGIC This table stores the email notifications that will be sent to contributors. Each record includes the recipient information, subject line, and message body summarizing validation or QC issues. These messages are processed later by Notebook 07 – Email Sender.
# MAGIC
# MAGIC opsChecklistActions  
# MAGIC This table records the checklist updates required when validation or QC issues are detected. For affected sheets, the checklist status is automatically set to “In Progress” so contributors know which sections of the workbook require correction.
# MAGIC
# MAGIC opsGoldCandidates  
# MAGIC This table identifies studies that passed all validation and QC checks and are eligible to proceed to the Gold layer. These studies can move forward to final integration and analysis-ready dataset generation.
# MAGIC
# MAGIC Together, these outputs translate validation results into actionable pipeline steps: contributors are notified of issues, checklist corrections are triggered, and fully validated studies are promoted to the next stage of the GEMS data pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 1 — Config

# COMMAND ----------

from datetime import datetime, timezone, timedelta
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

catalog = "gems_catalog"
schema  = "gems_schema"

# Inputs
stoplightTable = f"{catalog}.{schema}.gemsValidationStoplight"
qcSummaryTable = f"{catalog}.{schema}.silverQcSummary"
qcDetailsTable = f"{catalog}.{schema}.silverQcDetails"
emailQueueStoplightTable = f"{catalog}.{schema}.opsEmailQueueStoplight"   # from Notebook 3

# Outputs
emailQueueQcTable = f"{catalog}.{schema}.opsEmailQueueQc"                 # new queue for Notebook 6
checklistActionsTable = f"{catalog}.{schema}.opsChecklistActions"         # reuse
goldCandidatesTable = f"{catalog}.{schema}.opsGoldCandidates"             # optional helper table

# -----------------------------
# Email sending config (Gmail SMTP)
# -----------------------------
FROM_EMAIL = "no-reply@bovi-analytics.org"
REPLY_TO   = "no-reply@bovi-analytics.org"  # or a monitored inbox if you prefer

ALWAYS_CC = "pn287@cornell.edu"
FIX_WINDOW_DAYS = 7

emailRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Load latest Stoplight + latest QC

# COMMAND ----------

stop = spark.table(stoplightTable)
qcS  = spark.table(qcSummaryTable)

latestGateRun = stop.select(F.max("gateRunId").alias("gateRunId")).collect()[0]["gateRunId"]
latestQcRun   = qcS.select(F.max("qcRunId").alias("qcRunId")).collect()[0]["qcRunId"]

stop_latest = stop.filter(F.col("gateRunId") == latestGateRun)
qc_latest   = qcS.filter(F.col("qcRunId") == latestQcRun)

print("emailRunId:", emailRunId)
print("latestGateRun:", latestGateRun)
print("latestQcRun:", latestQcRun)

display(stop_latest.select("studyId","sequence","coreDataStatus","promisedDataStatus","status","coreDataMessage","promisedDataMessage").orderBy("studyId","sequence"))
display(qc_latest.select("studyId","sequence","qcStatus","qcMessage","ruleVersionUsed").orderBy("studyId","sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Build unified status table + email routing
# MAGIC ###### We reuse routing from opsEmailQueueStoplight (DM email + contributor CC logic already done).

# COMMAND ----------

route = spark.table(emailQueueStoplightTable)

# Only latest gate run rows from routing table
# (it already stores gateRunId; if not, we'll join by (studyId, sequence) only)
route_latest = route.filter(F.col("gateRunId") == latestGateRun) if "gateRunId" in route.columns else route

# Join status + routing + QC
u = (
    stop_latest.alias("s")
    .join(qc_latest.alias("q"), on=["studyId","sequence"], how="left")
    .join(route_latest.alias("r"), on=["studyId","sequence"], how="left")
    .select(
        F.col("studyId"), F.col("sequence"),
        F.col("s.contractName").alias("contractName"),
        F.col("s.workbookFile").alias("workbookFile"),
        F.col("s.workbookPath").alias("workbookPath"),

        F.col("s.coreDataStatus").alias("coreDataStatus"),
        F.col("s.coreDataMessage").alias("coreDataMessage"),
        F.col("s.promisedDataStatus").alias("promisedDataStatus"),
        F.col("s.promisedDataMessage").alias("promisedDataMessage"),

        F.coalesce(F.col("q.qcStatus"), F.lit("NOT_RUN")).alias("qcStatus"),
        F.coalesce(F.col("q.qcMessage"), F.lit("QC not executed for this study")).alias("qcMessage"),
        F.coalesce(F.col("q.ruleVersionUsed"), F.lit("")).alias("ruleVersionUsed"),

        # Routing fields
        F.col("r.toEmail").alias("toEmail"),
        F.col("r.ccEmails").alias("ccEmails"),
        F.col("r.dataManagerName").alias("dataManagerName"),
        F.col("r.contributorName").alias("contributorName"),
        F.col("r.contributorEmail").alias("contributorEmail"),
    )
)

display(u.orderBy("studyId","sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — Create email subject/body + checklist “In Progress” actions + gold candidates
# MAGIC
# MAGIC This cell produces 3 outputs:
# MAGIC
# MAGIC opsEmailQueueQc (emails to send later)
# MAGIC
# MAGIC opsChecklistActions (set sheets “In Progress” if any issues)
# MAGIC
# MAGIC opsGoldCandidates (eligible for Gold: core PASS + qc PASS)
# MAGIC
# MAGIC Build email queue + gold candidates + checklist actions (FIXED)

# COMMAND ----------

# Determine overall category
u2 = (
    u
    .withColumn("hasCoreIssue", F.col("coreDataStatus") != F.lit("PASS"))
    .withColumn("hasPromisedIssue", F.col("promisedDataStatus") != F.lit("PASS"))
    .withColumn("hasQcIssue", F.col("qcStatus") != F.lit("PASS"))
    .withColumn("allPass", (F.col("coreDataStatus") == "PASS") & (F.col("promisedDataStatus") == "PASS") & (F.col("qcStatus") == "PASS"))
)

# Subjects
u2 = u2.withColumn(
    "emailSubject",
    F.when(F.col("hasCoreIssue"),
           F.lit("[GEMS] Action required: core project data missing (study not eligible)"))
     .when(F.col("allPass"),
           F.lit("[GEMS] Congratulations: your study passed validation and QC"))
     .otherwise(F.lit("[GEMS] Action required: promised data and/or QC issues"))
)

# Fix deadline text
fix_deadline = (datetime.now(timezone.utc) + timedelta(days=FIX_WINDOW_DAYS)).strftime("%Y-%m-%d")

# -------------------------------------------------
# Attach workbookFile from validation stoplight (use same runId!)
# -------------------------------------------------
stoplightTable = "gems_catalog.gems_schema.gemsValidationStoplight"

stop_meta = (
    spark.table(stoplightTable)
         .filter(F.col("gateRunId") == F.lit(latestGateRun))
         .select(
             "studyId",
             "sequence",
             F.col("workbookFile").alias("workbookFileStoplight")   # <-- rename
         )
         .dropDuplicates()
)

u2 = u2.join(stop_meta, ["studyId","sequence"], "left")

# Choose the workbookFile you want to use in emails:
# Prefer stoplight value; fallback to any existing workbookFile in u2 if present.
u2 = u2.withColumn(
    "workbookFile",
    F.coalesce(F.col("workbookFileStoplight"), F.col("workbookFile"))
).drop("workbookFileStoplight")

# Build email body
# Optional: a short definition footer used in multiple branches
core_def = (
    "Core data are the project-required data types and variables that are required by GEMS, "
    "as indicated (ticked) in the DUA attachment/table."
)
prom_def = (
    "Promised data are the additional datasets you indicated in the DUA summary you would contribute "
    "(beyond the core required items)."
)

# Build email body — each logical section separated by blank lines for readability
# Section separator: double newline; list items: single newline within a section
u2 = u2.withColumn(
    "emailBody",
    F.when(
        F.col("hasCoreIssue"),
        F.concat_ws("\n\n",

            F.lit("Hello,"),

            F.concat(
                F.lit("Study ID: "), F.col("studyId"),
                F.lit(" | DUA Study #: "), F.col("sequence"),
                F.lit(" | Workbook: "), F.col("workbookFile")
            ),

            F.lit("Your study **did NOT pass the GEMS core data requirement check**."),

            F.lit(core_def),

            F.lit("Core project-required datasets must be delivered to participate in downstream analyses and payment eligibility."),

            F.concat_ws("\n",
                F.lit("**Core data issue(s):**"),
                F.concat(F.lit("- "), F.col("coreDataMessage"))
            ),

            F.concat_ws("\n",
                F.lit("**Next steps**"),
                F.concat(
                    F.lit("- Please correct the workbook and complete the missing core datasets by **"),
                    F.lit(fix_deadline),
                    F.lit("**.")
                ),
                F.lit("- Relevant sheets will be set to 'In Progress'."),
                F.lit("- Once corrections are completed, change the sheet status to 'Completed' in the Checklist."),
                F.lit("- The workbook will then be processed again and you will receive an updated notification.")
            ),

            F.lit("Thank you,"),
            F.lit("GEMS Coordination Team")
        )
    ).when(
        F.col("allPass"),
        F.concat_ws("\n\n",

            F.lit("Hello,"),

            F.concat(
                F.lit("Study ID: "), F.col("studyId"),
                F.lit(" | DUA Study #: "), F.col("sequence"),
                F.lit(" | Workbook: "), F.col("workbookFile")
            ),

            F.lit("🎉 **Congratulations.** Your study successfully passed all validation checks:"),

            F.concat_ws("\n",
                F.lit("- Core data requirement check"),
                F.lit("- Promised data validation"),
                F.lit("- Quality control (QC) checks")
            ),

            F.lit(core_def),

            F.lit(prom_def),

            F.lit("Your study is now eligible for the **Gold layer** of the GEMS data pipeline. Gold studies are not reprocessed unless updates are detected in the workbook."),

            F.lit("Thank you for contributing data to the GEMS initiative."),

            F.lit("Best regards,"),
            F.lit("GEMS Coordination Team")
        )
    ).otherwise(
        F.concat_ws("\n\n",

            F.lit("Hello,"),

            F.concat(
                F.lit("Study ID: "), F.col("studyId"),
                F.lit(" | DUA Study #: "), F.col("sequence"),
                F.lit(" | Workbook: "), F.col("workbookFile")
            ),

            F.lit("Your study **passed the core data requirement check**, but additional issues remain before the study can be fully accepted."),

            F.lit(core_def),

            F.lit(prom_def),

            F.when(
                F.col("hasPromisedIssue"),
                F.concat_ws("\n",
                    F.lit("**Promised data issue(s):**"),
                    F.concat(F.lit("- "), F.col("promisedDataMessage")),
                    F.lit("You indicated in the DUA summary that you would contribute these datasets.")
                )
            ).otherwise(
                F.lit("Promised data validation: PASS")
            ),

            F.when(
                F.col("hasQcIssue"),
                F.concat_ws("\n",
                    F.lit("**QC issue(s):**"),
                    F.concat(F.lit("- "), F.col("qcMessage"))
                )
            ).otherwise(
                F.lit("Quality control (QC): PASS")
            ),

            F.concat_ws("\n",
                F.lit("**Next steps**"),
                F.concat(
                    F.lit("- Please correct the workbook by **"),
                    F.lit(fix_deadline),
                    F.lit("**.")
                ),
                F.lit("- Sheets with missing promised data or QC issues will be set to 'In Progress'."),
                F.lit("- After corrections, update the sheet status to 'Completed' in the Checklist."),
                F.lit("- The workbook will then be processed again and you will receive a follow-up email.")
            ),

            F.lit("Thank you,"),

            F.lit("GEMS Coordination Team")
        )
    )
)

# Ensure ALWAYS_CC is included (avoid duplicates)
u2 = u2.withColumn(
    "ccEmailsFinal",
    F.when(
        (F.col("ccEmails").isNull()) | (F.trim(F.col("ccEmails")) == ""),
        F.lit(ALWAYS_CC)
    ).otherwise(
        F.when(
            F.instr(F.lower(F.col("ccEmails")), F.lower(F.lit(ALWAYS_CC))) > 0,
            F.col("ccEmails")
        ).otherwise(F.concat_ws(",", F.col("ccEmails"), F.lit(ALWAYS_CC)))
    )
)

# Sanitize email/body fields: remove null bytes (\\x00), NBSP (\\xa0), control chars
# that cause UnicodeEncodeError when sending via SMTP (from Excel/DUA corruption)
def strip_control_chars(col):
    """Remove \\x00, \\xa0, and control chars 0x00-0x1F, 0x7F (except \\n and \\r)."""
    return F.regexp_replace(F.coalesce(col, F.lit("")), "[\\x00-\\x1F\\x7F\\xA0]", "")


def strip_control_chars_preserve_newlines(col):
    """Same as strip_control_chars but preserves \\n (0x0A) and \\r (0x0D) for email body formatting."""
    return F.regexp_replace(F.coalesce(col, F.lit("")), "[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F\\xA0]", "")

# Email queue dataframe
emailDf = (
    u2
    .select(
        F.lit(emailRunId).alias("emailRunId"),
        F.lit(latestGateRun).alias("gateRunId"),
        F.lit(latestQcRun).alias("qcRunId"),
        "studyId","sequence",
        strip_control_chars(F.col("toEmail")).alias("toEmail"),
        strip_control_chars(F.col("ccEmailsFinal")).alias("ccEmails"),
        F.lit(FROM_EMAIL).alias("fromEmail"),
        F.lit(REPLY_TO).alias("replyTo"),
        "dataManagerName","contributorName","contributorEmail",
        strip_control_chars(F.col("emailSubject")).alias("emailSubject"),
        strip_control_chars_preserve_newlines(F.col("emailBody")).alias("emailBody"),
        "coreDataStatus","promisedDataStatus","qcStatus",
        "coreDataMessage","promisedDataMessage","qcMessage",
        "workbookFile",
        F.lit(datetime.now(timezone.utc).isoformat()).alias("createdTsUtc")
    )
)

# Gold candidates (core PASS + QC PASS)
goldDf = (
    u2.filter(
        (F.col("coreDataStatus") == "PASS") &
        (F.col("promisedDataStatus") == "PASS") &
        (F.col("qcStatus") == "PASS")
    )
    .select(
        F.lit(emailRunId).alias("emailRunId"),
        F.lit(latestGateRun).alias("gateRunId"),
        F.lit(latestQcRun).alias("qcRunId"),
        "studyId","sequence",
        "promisedDataStatus","promisedDataMessage",
        F.lit(datetime.now(timezone.utc).isoformat()).alias("createdTsUtc")
    )
)

# -----------------------------
# Checklist actions
# -----------------------------
def explode_sheets_from_message(df, msg_col, issue_flag_col, issue_type):
    parts = df.filter(F.col(issue_flag_col)).select("studyId","sequence", F.col(msg_col).alias("msg"))
    exploded = (
        parts
        .withColumn("part", F.explode(F.split(F.col("msg"), r"\s*\|\s*")))
        .withColumn("sheetCandidate", F.trim(F.regexp_extract(F.col("part"), r"^([^:]+):", 1)))
        .filter(F.col("sheetCandidate") != "")
        .select("studyId","sequence", F.col("sheetCandidate").alias("sheetName"))
        .dropDuplicates()
        .withColumn("action", F.lit("set_checklist_in_progress"))
        .withColumn("reason", F.lit(issue_type))
        .withColumn("createdTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))
    )
    return exploded

coreSheets = explode_sheets_from_message(u2, "coreDataMessage", "hasCoreIssue", "core_data_issue")
promSheets = explode_sheets_from_message(u2, "promisedDataMessage", "hasPromisedIssue", "promised_data_issue")

# QC sheets: from QC details
qc_details_latest = spark.table(qcDetailsTable).filter(F.col("qcRunId") == latestQcRun)
qcSheets = (
    u2.filter(F.col("hasQcIssue"))
      .select("studyId","sequence")
      .join(qc_details_latest.select("studyId","sequence","sourceSheet").dropDuplicates(), on=["studyId","sequence"], how="left")
      .filter(F.col("sourceSheet").isNotNull())
      .select(
          "studyId","sequence",
          F.col("sourceSheet").alias("sheetName")
      )
      .dropDuplicates()
      .withColumn("action", F.lit("set_checklist_in_progress"))
      .withColumn("reason", F.lit("qc_issue"))
      .withColumn("createdTsUtc", F.lit(datetime.now(timezone.utc).isoformat()))
)

actionsDf = coreSheets.unionByName(promSheets).unionByName(qcSheets).dropDuplicates()

# ✅ FIX: match opsChecklistActions schema exactly
actionsDf_fixed = (
    actionsDf
    .withColumn("gateRunId", F.lit(latestGateRun))
    .select(
        F.col("gateRunId").cast("string"),
        F.col("studyId").cast("string"),
        F.col("sequence").cast("string"),
        F.col("sheetName").cast("string"),
        F.col("action").cast("string"),
        F.col("reason").cast("string"),
        F.col("createdTsUtc").cast("string"),
    )
)

# -----------------------------
# Write outputs
# -----------------------------
emailDf.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(emailQueueQcTable)
goldDf.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(goldCandidatesTable)

# Use the fixed schema df here
actionsDf_fixed.write.format("delta").mode("append").saveAsTable(checklistActionsTable)

print("Notebook 6 prepared email queue + actions.")
print("emailRunId:", emailRunId)          # <-- ADD THIS LINE
print("emailQueueQcTable:", emailQueueQcTable)
print("goldCandidatesTable:", goldCandidatesTable)
print("checklistActionsTable:", checklistActionsTable)

display(emailDf.orderBy("studyId","sequence"))
display(actionsDf_fixed.orderBy("studyId","sequence","sheetName"))
display(goldDf.orderBy("studyId","sequence"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — What to send (filters) + sanity checks

# COMMAND ----------

emails = spark.table(emailQueueQcTable).filter(F.col("emailRunId") == emailRunId)

print("Emails prepared:", emails.count())
display(emails.select("studyId","sequence","workbookFile","toEmail","emailSubject","coreDataStatus","promisedDataStatus","qcStatus").orderBy("studyId","sequence"))

print("Gold candidates prepared:", spark.table(goldCandidatesTable).filter(F.col("emailRunId") == emailRunId).count())