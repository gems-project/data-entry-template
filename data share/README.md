# GEMS Delta Sharing — download shared tables (Python & R)

This folder contains two scripts that connect to a **Databricks Delta Sharing** share using a small credential file you get from us. They list every table you are allowed to see, download the data, and save files you can open in Excel or a browser.

| File | Language | What it does |
|------|----------|----------------|
| `load_shared_table.py` | Python | Installs missing packages if needed, exports **Excel (`.xlsx`)** and **HTML (`.html`)** per table |
| `load_shared_table.R` | R | Installs missing CRAN packages if needed, exports **Excel (`.xlsx`)** per table (Parquet-backed shares only) |

---

## 1. What you get from us

We send you an **activation link**. Download the file it provides and save it as:

**`config.share`**

(You can rename it to **`config.json`** if you prefer — both names work.)

**Important:** Save `config.share` (or `config.json`) in the **same folder** as `load_shared_table.py` and `load_shared_table.R` — the folder from which you run the script (see below).

---

## 2. You only need to download the config **once**

The config file is **not a copy of the data**. It only holds:

- the **server address** (endpoint),
- your **access token**,
- and which **share** you may read.

Think of it like this:

- **`config.share`** = key to the door  
- **share** = shared “database” we maintain  
- **tables** = the actual datasets inside that share  

When we **add new tables**, **update schemas**, or **append rows** to tables in the **same** share, you **do not** need a new file. The next time you run the script, it asks the server what tables exist **right now** and downloads the current data.

You need a **new** download only in situations such as:

- the **token expired** (we can set long lifetimes to reduce this),
- we created a **new recipient** for you and issued **new** credentials,
- we **rotated** your token for security,
- or you were given access to a **different share** that requires a **different** profile.

**Tip for our team:** keep one stable share (e.g. one share name) and add or update **tables** inside it, instead of creating a new share or new recipient for every change — that avoids forcing everyone to re-download configs.

---

## 3. How to run (after the config is in this folder)

Open a terminal (PowerShell, Command Prompt, Terminal on Mac/Linux), **change directory** to this folder (the one that contains the scripts and `config.share`), then run **one** of the following.

### Python

```bash
python load_shared_table.py
```

On some computers the command is `python3` instead of `python`.

**First run:** the script may install packages automatically (`pandas`, `pyarrow`, `openpyxl`, `delta-sharing`). That can take a minute or two and needs internet access.

### R

```bash
Rscript load_shared_table.R
```

**First run:** R may install packages from CRAN (`jsonlite`, `httr2`, `arrow`, `writexl`). Again, internet is required.

**From inside R (e.g. RStudio):** the auto-run at the bottom of the script is tied to `Rscript`. After sourcing, call the loader yourself:

```r
setwd("/path/to/this/folder")   # folder that contains the scripts + config.share
source("load_shared_table.R")
tabs <- load_gems_tables()
```

---

## 4. Getting **updated** data

Nothing special: **run the same command again** (same `config.share` in the same place). The scripts always fetch the **current** list of tables and the **current** data the server exposes.

---

## 5. Where files are written

Both scripts write under a subfolder next to the scripts:

**`shared_table_exports/`**

- **Python:** for each table, one **`.xlsx`** and one **`.html`** (open the HTML in any web browser).
- **R:** one **`.xlsx`** per table (no HTML in the R script).

File names look like: `shareName__schemaName__tableName.xlsx` (special characters are sanitized).

The scripts also remove a few **internal pipeline columns** from the exports (for example lineage fields such as `sequence`, `workbookFile`, `workbookPath`, `gateRunId`, `ingestRunId`) so the files are easier to use for analysis.

---

## 6. Optional: use the scripts as a library (examples)

### Python (in a notebook or another script)

You can run the file as-is, or reuse the same idea with the official client:

```python
from pathlib import Path
from delta_sharing import SharingClient, load_as_pandas

profile = str(Path("config.share").resolve())  # or config.json
client = SharingClient(profile)
tables = list(client.list_all_tables())
for t in tables:
    url = f"{profile}#{t.share}.{t.schema}.{t.name}"
    df = load_as_pandas(url)
    print(t.name, df.shape)
```

(If you use this pattern, install the same packages as in `load_shared_table.py`.)

### R (after `source("load_shared_table.R")`)

```r
# All tables as a named list of data.frames; also writes .xlsx under shared_table_exports/
all <- load_gems_tables()

# First table only
one <- load_gems_table()

# One table by export stem name (see filenames in shared_table_exports)
one <- load_gems_table("myShare__mySchema__myTable")

# List what the share exposes (share / schema / name)
gems_list_tables()
```

**R limitation:** the R script uses the Delta Sharing REST API and **Parquet** file URLs. If the server responds in **Delta** response format only, this R code will error; the **Python** script is the more general option in that case.

---

## 7. If something goes wrong

These scripts have been **tested** end-to-end with a valid profile: when the profile is correct, the network allows access, and Python/R can install packages, downloads succeed.

If you hit errors:

1. Confirm **`config.share`** (or **`config.json`**) is in the **same folder** as the script you run, and that your terminal’s **current directory** is that folder (or use `setwd` in R).
2. Check **internet** and any **VPN** or firewall rules.
3. If the error mentions **401 / 403** or **expired**, ask us for a **new** activation file.
4. Use an **AI assistant** (or your IDE’s AI) with the **full error message** and this README — that often speeds up debugging.

---

## 8. Security

Treat **`config.share`** like a password. **Do not** commit it to GitHub or share it in public channels. Only share the **repository link** that contains these **scripts** and this **README**; each collaborator should receive their own credential file through a **private** channel.
