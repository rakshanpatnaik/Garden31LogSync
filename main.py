"""
tend_export_to_two_tables.py

Reads a Tend export CSV with multiple sections (Container Sow / Transplant / Precision Sow)
and writes to two Supabase tables:

1) gh_planting_log: Container Sow rows
2) row_planting_log: Transplant + Precision Sow rows with these columns:
   - Plant Name        (from Planting)
   - Variety           (from Planting)
   - Location          (from Location)
   - Spacing           (from In-row Spacing)
   - Direct/Transplant (Transplant -> "Transplant", Precision Sow -> "Direct")

Required CSV columns (in section headers):
- Task Id
- Task Type
- Start Date
- Planting
- Seeds Needed
- Location
- In-row Spacing

Env:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  SUPABASE_TABLE_GH   (default: gh_planting_log)
  SUPABASE_TABLE_ROW  (default: row_planting_log)

Run:
  pip install pandas python-dateutil supabase
  python tend_export_to_two_tables.py "/path/to/ExportTask.csv"
"""

import os
import sys
import csv
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
from dateutil import parser as dateparser
from supabase import create_client

load_dotenv()

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# =========================
# Microsoft Graph helpers
# =========================

def get_graph_token() -> str:
    token_url = (
        f"https://login.microsoftonline.com/{os.environ['MS_TENANT_ID']}/oauth2/v2.0/token"
    )
    data = {
        "client_id": os.environ["MS_CLIENT_ID"],
        "client_secret": os.environ["MS_CLIENT_SECRET"],
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(token_url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


def list_csv_files(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    mode = os.environ.get("MS_DRIVE_MODE", "onedrive").lower()

    if mode == "onedrive":
        user = os.environ["ONEDRIVE_USER_PRINCIPAL_NAME"]
        folder = os.environ["ONEDRIVE_FOLDER_PATH"]
        url = f"{GRAPH_BASE}/users/{user}/drive/root:{folder}:/children"
    else:
        site_id = os.environ["SP_SITE_ID"]
        drive_id = os.environ["SP_DRIVE_ID"]
        folder = os.environ["SP_FOLDER_PATH"]
        url = f"{GRAPH_BASE}/sites/{site_id}/drives/{drive_id}/root:{folder}:/children"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    # Only CSVs
    return [it for it in items if it.get("name", "").lower().endswith(".csv")]


def fetch_latest_csv() -> Optional[str]:
    """Download the latest CSV from OneDrive/SharePoint to a temp file and return its path."""
    token = get_graph_token()
    csv_items = list_csv_files(token)
    if not csv_items:
        print("No CSV files found in the configured folder.")
        return None

    latest = sorted(
        csv_items,
        key=lambda it: dateparser.parse(it["lastModifiedDateTime"]),
        reverse=True,
    )[0]

    headers = {"Authorization": f"Bearer {token}"}
    drive_id = latest["parentReference"]["driveId"]
    file_id = latest["id"]
    download_url = f"{GRAPH_BASE}/drives/{drive_id}/items/{file_id}/content"

    resp = requests.get(download_url, headers=headers)
    resp.raise_for_status()

    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "wb") as f:
        f.write(resp.content)

    print(f"Downloaded latest CSV: {latest['name']} → {path}")
    return path




# ---------- Helper Functions ----------

def parse_date(value) -> Optional[str]:
    # Handles None, NaN, empty strings
    if value is None or pd.isna(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    # parses CSV in MM/DD/YYYY form
    try:
        return datetime.strptime(s, "%m/%d/%Y").date().isoformat()
    except Exception:
        pass

    # Fallback: try dateutil for any odd formats
    try:
        return dateparser.parse(s).date().isoformat()
    except Exception:
        return None

# Return cleaned string
def to_number(value) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return s.replace(",", "")
    except Exception:
        return None

# Splits Planting column in CSV to "PLant Name" and "Variety"
def split_planting(value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Example:
      "Beans (Common) - Dragon's Tongue - Seedlings / Plugs"
    -> Plant Name = "Beans (Common)"
    -> Variety    = "Dragon's Tongue"
    """
    if value is None:
        return (None, None)
    s = str(value).strip()
    if not s:
        return (None, None)
    parts = [p.strip() for p in s.split(" - ") if p.strip()]
    plant_name = parts[0] if len(parts) >= 1 else None
    variety = parts[1] if len(parts) >= 2 else None
    return (plant_name, variety)


def clean_headers(headers: List[str]) -> List[str]:
    headers = [h.strip() if h is not None else "" for h in headers]
    while headers and headers[-1] == "":
        headers.pop()
    return headers

# Parses each row of the CSV into dictionary using headers as keys
def row_to_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    if len(row) > len(headers):
        row = row[: len(headers)]
    elif len(row) < len(headers):
        row = row + [""] * (len(headers) - len(row))
    return {headers[i]: row[i] for i in range(len(headers))}

# Going through and dividing the CSV file into multiple sections (Container Sow --> GH; Transplant, Precision Sow --> Row)
def read_tend_multisection_csv(path: str) -> pd.DataFrame:
    """
    Reads Tend export CSVs that contain multiple sections with repeated headers.
    Collects all data rows after each 'Task Id' header line.
    """
    all_rows: List[Dict[str, str]] = []
    current_headers: Optional[List[str]] = None

    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue

            first = (row[0] or "").strip()

            # Header line for a section
            if first == "Task Id":
                current_headers = clean_headers(row)
                continue

            # Until we see first header, ignore lines (section titles, blanks, etc.)
            if current_headers is None:
                continue

            rec = row_to_dict(current_headers, row)

            # Skip non-data rows
            if not rec.get("Task Id"):
                continue

            all_rows.append(rec)

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------- Transform ----------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "Task Id",
        "Task Type",
        "Start Date",
        "Planting",
        "Seeds Needed",
        "Location",
        "In-row Spacing",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in parsed data: {sorted(missing)}")

    plant_name, variety = zip(*df["Planting"].map(split_planting))

    # Supabase Column Name : CSV Column Name mapping
    out = pd.DataFrame(
        {
            "Tend ID": df["Task Id"].astype(str).str.strip(),
            "task_type": df["Task Type"].astype(str).str.strip(), # not a supabase column, meant to map rows into either Direct or Transplant for Direct/Transplant column
            "Date": df["Start Date"].map(parse_date),
            "Plant Name": pd.Series(plant_name, dtype="string"),
            "Variety": pd.Series(variety, dtype="string"),
            "Quantity": df["Seeds Needed"].map(to_number),
            "Location": df["Location"].astype(str).str.strip(),
            "Spacing": df["In-row Spacing"].map(to_number),
        }
    )

    out = out.replace({"": None})
    out = out.where(pd.notnull(out), None)
    out = out.dropna(subset=["Tend ID"])

    return out

# Inserting row-by-row into Supabase
def upsert_table(sb, table: str, rows: List[dict], conflict_col: str = "Tend ID"):
    if not rows:
        print(f"[{table}] No rows to upsert.")
        return
    sb.table(table).upsert(rows, on_conflict=conflict_col).execute()
    print(f"[{table}] Upserted {len(rows)} rows (on_conflict={conflict_col}).")


# ---------- Main ----------

def main():
    print("Fetching latest CSV from OneDrive/SharePoint...")

    csv_path = fetch_latest_csv()
    if not csv_path:
        print("No CSV files found in the configured folder. Exiting.")
        return

    # Supabase config
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    table_gh = os.environ.get("SUPABASE_TABLE_GH", "gh_planting_log")
    table_row = os.environ.get("SUPABASE_TABLE_ROW", "row_planting_log")

    sb = create_client(supabase_url, supabase_key)

    # Parse CSV into sections
    raw = read_tend_multisection_csv(csv_path)
    if raw.empty:
        print("No rows found in CSV after parsing.")
        return

    norm = transform(raw)

    # ---- gh_planting_log: Container Sow ----
    gh_df = norm[norm["task_type"].str.lower() == "container sow"].copy()

    gh_rows = gh_df[
        ["Tend ID", "Date", "Plant Name", "Variety", "Quantity"]
    ].to_dict(orient="records")

    upsert_table(sb, table_gh, gh_rows, conflict_col="Tend ID")

    # ---- row_planting_log: Transplant + Precision Sow ----
    row_df = norm[
        norm["task_type"].str.lower().isin(["transplant", "precision sow"])
    ].copy()

    def map_direct_transplant(tt: Optional[str]) -> Optional[str]:
        if tt is None:
            return None
        t = tt.strip().lower()
        if t == "transplant":
            return "Transplant"
        if t == "precision sow":
            return "Direct"
        return None

    row_df["Direct/Transplant"] = row_df["task_type"].map(map_direct_transplant)

    row_payload = row_df[
        ["Tend ID", "Date", "Plant Name", "Variety", "Location", "Spacing", "Direct/Transplant"]
    ].to_dict(orient="records")

    upsert_table(sb, table_row, row_payload, conflict_col="Tend ID")

    print("\nSummary:")
    print(f"  Parsed rows total: {len(norm)}")
    print(f"  gh_planting_log (Container Sow): {len(gh_df)}")
    print(f"  row_planting_log (Transplant/Precision Sow): {len(row_df)}")



if __name__ == "__main__":
    main()