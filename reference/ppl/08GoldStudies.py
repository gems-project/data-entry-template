# Databricks notebook source
# MAGIC %md
# MAGIC #### Cell 1 — Config

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

catalog = "gems_catalog"
schema  = "gems_schema"

silverPrefix = f"{catalog}.{schema}.silver"
goldPrefix   = f"{catalog}.{schema}.gold"

goldCandidatesTable = f"{catalog}.{schema}.opsGoldCandidates"
goldStudiesTable    = f"{catalog}.{schema}.opsGoldStudies"
goldLogTable        = f"{catalog}.{schema}.opsGoldBuildLog"

silverLogTable      = f"{catalog}.{schema}.opsSilverTransformLog"

goldRunId = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 2 — Identify latest Gold candidates

# COMMAND ----------

candidates = spark.table(goldCandidatesTable)

latestEmailRun = (
    candidates
    .select(F.max("emailRunId").alias("emailRunId"))
    .collect()[0]["emailRunId"]
)

goldCandidates = (
    candidates
    .filter(F.col("emailRunId") == latestEmailRun)
    .select("studyId","sequence")
    .dropDuplicates()
)

display(goldCandidates)
print("Gold candidate studies:", goldCandidates.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 3 — Identify studies already promoted to Gold

# COMMAND ----------

if spark.catalog.tableExists(goldStudiesTable):

    existingGold = spark.table(goldStudiesTable) \
        .select("studyId","sequence") \
        .dropDuplicates()

else:

    existingGold = spark.createDataFrame([], "studyId string, sequence string, workbookFile string")

display(existingGold)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 4 — Detect which candidates need Gold build
# MAGIC We only rebuild if:
# MAGIC
# MAGIC study not yet promoted
# MAGIC
# MAGIC OR workbook file changed

# COMMAND ----------

silverLog = spark.table(silverLogTable)

latestSilver = (
    silverLog
    .groupBy("studyId","sequence")
    .agg(F.max("silverRunId").alias("silverRunId"))
)

silverStudies = (
    silverLog.alias("l")
    .join(latestSilver.alias("r"),["studyId","sequence","silverRunId"])
    .select("studyId","sequence","silverRunId")
)

goldWork = (
    goldCandidates.alias("c")
    .join(silverStudies.alias("s"),["studyId","sequence"])
    .join(existingGold.alias("g"),["studyId","sequence"],"left")
)

display(goldWork)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 5 — Discover Silver tables

# COMMAND ----------

def list_silver_tables():

    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

    out = []

    for r in rows:
        t = r["tableName"]

        if t.lower().startswith("silver"):
            out.append(f"{catalog}.{schema}.{t}")

    return sorted(out)

silverTables = list_silver_tables()

for t in silverTables:
    print(t)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 6 — Promote Silver → Gold

# COMMAND ----------

import pandas as pd

logRows = []

# -------------------------------------------------------
# Step 1 — Collect candidate studies (remove duplicates)
# -------------------------------------------------------

goldCandidatesRows = (
    goldWork
    .select("studyId","sequence","silverRunId")
    .dropDuplicates(["studyId","sequence"])
    .collect()
)

candidateStudies = [(r["studyId"], r["sequence"]) for r in goldCandidatesRows]

print("Studies to promote:", candidateStudies)

if len(candidateStudies) == 0:
    print("No studies require Gold promotion.")
else:

    # Convert to dataframe for filtering
    candidateDf = spark.createDataFrame(candidateStudies, ["studyId","sequence"])

    # -------------------------------------------------------
    # Step 2 — Process each Silver table once
    # -------------------------------------------------------

    for st in silverTables:

        suffix = st.split(".")[-1][len("silver"):]
        goldTable = f"{goldPrefix}{suffix}"

        print(f"Processing table: {st} -> {goldTable}")

        try:

            # Read Silver table once
            sdf = spark.table(st)

            # Keep only candidate studies
            filtered = (
                sdf.join(
                    candidateDf,
                    on=["studyId","sequence"],
                    how="inner"
                )
            )

            # Remove duplicated rows before writing to Gold
            filtered = filtered.dropDuplicates(filtered.columns)

            # Skip if nothing to write
            if filtered.rdd.isEmpty():
                print("No rows found for candidate studies.")
                continue

            # Build replaceWhere clause
            replaceClause = " OR ".join(
                [f"(studyId='{s}' AND sequence='{q}')" for s,q in candidateStudies]
            )

            # Write to Gold table
            (
                filtered.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", replaceClause)
                .saveAsTable(goldTable)
            )

            # Log success
            for s,q in candidateStudies:

                logRows.append({
                    "goldRunId": goldRunId,
                    "studyId": s,
                    "sequence": q,
                    "silverTable": st,
                    "goldTable": goldTable,
                    "status": "SUCCESS",
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })

        except Exception as e:

            print("ERROR:", e)

            # Log failure
            for s,q in candidateStudies:

                logRows.append({
                    "goldRunId": goldRunId,
                    "studyId": s,
                    "sequence": q,
                    "silverTable": st,
                    "goldTable": goldTable,
                    "status": "FAILED",
                    "errorMessage": str(e),
                    "createdTsUtc": datetime.now(timezone.utc).isoformat()
                })

print("Gold promotion finished")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 7 — Record Gold Studies
# MAGIC
# MAGIC Record which studies were promoted to Gold so future pipeline runs can track them.

# COMMAND ----------

goldRows = []

# collect unique studies that were promoted
goldStudiesRows = (
    goldWork
    .select("studyId","sequence","silverRunId")
    .dropDuplicates(["studyId","sequence"])
    .collect()
)

for r in goldStudiesRows:

    goldRows.append({
        "goldRunId": goldRunId,
        "studyId": r["studyId"],
        "sequence": r["sequence"],
        "silverRunId": r["silverRunId"],
        "createdTsUtc": datetime.now(timezone.utc).isoformat()
    })

# write results if there are rows
if len(goldRows) == 0:

    print("No Gold study records to write.")

else:

    goldDf = spark.createDataFrame(pd.DataFrame(goldRows))

    (
        goldDf.write
        .format("delta")
        .mode("append")
        .saveAsTable(goldStudiesTable)
    )

    display(goldDf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cell 8 — Write Gold Build Log
# MAGIC
# MAGIC Record detailed logs for every table processed during Gold promotion.

# COMMAND ----------

if len(logRows) == 0:

    print("No Gold log entries to write.")

else:

    logDf = spark.createDataFrame(pd.DataFrame(logRows))

    (
        logDf.write
        .format("delta")
        .mode("append")
        .saveAsTable(goldLogTable)
    )

    display(logDf)

print("Gold build complete. goldRunId:", goldRunId)