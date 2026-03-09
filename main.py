from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ────────────────────────────────────────────────────────────────
SHEET_ID   = os.environ.get("SHEET_ID", "1mc_ofkHODCLuhFlV4w8-D3c8KeMYPkIooYpQh0pqLAM")
SCOPES     = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS")  # set as Railway env var

DEPT_TABS = [
    "REPERAGES", "VHR", "PRODUCTION", "CATERING",
    "BAR", "ARTISTIQUE", "TECHNIQUE", "SCENO",
    "PARTENARIAT", "COMMUNICATION"
]

# Column mapping (0-indexed): A=0 section, B=1 ligne, C=2 obs, D=3 typ,
# E=4 tva, F=5 est_ht, G=6 est_ttc, H=7 rh, I=8 reel_ht, J=9 reel_ttc,
# K=10 ecart, L=11 statut
COL = {
    "section": 0, "ligne": 1, "observations": 2, "typ": 3, "tva": 4,
    "est_ht": 5, "est_ttc": 6, "rh": 7, "reel_ht": 8, "reel_ttc": 9,
    "ecart": 10, "statut": 11
}

# ── Google Sheets client ──────────────────────────────────────────────────
def get_sheets_service():
    if not CREDS_JSON:
        raise HTTPException(status_code=500, detail="GOOGLE_CREDENTIALS env var not set")
    creds_dict = json.loads(CREDS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

# ── Helpers ───────────────────────────────────────────────────────────────
def safe_float(v):
    try:
        return float(str(v).replace(" ", "").replace(",", ".")) if v not in (None, "", "—") else 0.0
    except:
        return 0.0

def parse_dept_rows(values: list, tab: str) -> list:
    """Parse raw sheet rows into structured line items, skip header rows."""
    rows = []
    current_section = ""
    for i, row in enumerate(values):
        if i < 2:  # skip title + header rows
            continue
        # Pad row to 12 cols
        row = list(row) + [""] * (12 - len(row))

        section_val = str(row[COL["section"]]).strip()
        ligne_val   = str(row[COL["ligne"]]).strip()

        # Section header rows: merged cell, ligne is empty or same as section
        if section_val and not ligne_val:
            current_section = section_val
            continue
        # Skip total rows
        if "TOTAL" in ligne_val.upper():
            continue
        # Skip completely empty rows
        if not ligne_val and not any(str(c).strip() for c in row[5:10]):
            continue

        rows.append({
            "section":      current_section,
            "ligne":        ligne_val or f"(ligne {i})",
            "observations": str(row[COL["observations"]]).strip(),
            "typ":          str(row[COL["typ"]]).strip(),
            "tva":          str(row[COL["tva"]]).strip(),
            "est_ht":       safe_float(row[COL["est_ht"]]),
            "rh":           str(row[COL["rh"]]).strip(),
            "reel_ht":      safe_float(row[COL["reel_ht"]]),
            "statut":       str(row[COL["statut"]]).strip(),
            "_row":         i + 1  # 1-indexed sheet row, needed for write-back
        })
    return rows

# ── App ───────────────────────────────────────────────────────────────────
app = FastAPI(title="BudgetFlow API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # lock down to your GitHub Pages URL in production
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── GET /api/budget ───────────────────────────────────────────────────────
@app.get("/api/budget")
def get_budget():
    """Read all dept tabs and return full budget as JSON."""
    try:
        svc = get_sheets_service()
        result = {}

        for tab in DEPT_TABS:
            resp = svc.values().get(
                spreadsheetId=SHEET_ID,
                range=f"{tab}!A:L",
                valueRenderOption="UNFORMATTED_VALUE"
            ).execute()
            values = resp.get("values", [])
            result[tab] = parse_dept_rows(values, tab)

        return {"status": "ok", "data": result}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── GET /api/budget/{dept} ────────────────────────────────────────────────
@app.get("/api/budget/{dept}")
def get_dept(dept: str):
    """Read a single dept tab."""
    tab = dept.upper()
    if tab not in DEPT_TABS:
        raise HTTPException(status_code=404, detail=f"Dept '{dept}' not found")
    try:
        svc = get_sheets_service()
        resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{tab}!A:L",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        values = resp.get("values", [])
        return {"status": "ok", "dept": tab, "data": parse_dept_rows(values, tab)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── POST /api/budget/update ───────────────────────────────────────────────
class UpdateCell(BaseModel):
    dept:   str          # e.g. "PRODUCTION"
    row:    int          # 1-indexed sheet row
    field:  str          # "est_ht" | "reel_ht" | "statut"
    value:  str          # new value as string

FIELD_TO_COL = {"est_ht": "F", "reel_ht": "I", "statut": "L"}

@app.post("/api/budget/update")
def update_cell(payload: UpdateCell):
    """Write a single cell back to the Sheet."""
    tab = payload.dept.upper()
    if tab not in DEPT_TABS:
        raise HTTPException(status_code=404, detail=f"Dept '{tab}' not found")
    col = FIELD_TO_COL.get(payload.field)
    if not col:
        raise HTTPException(status_code=400, detail=f"Field '{payload.field}' not writable")
    try:
        svc = get_sheets_service()
        cell_range = f"{tab}!{col}{payload.row}"
        # Convert numeric strings to float for numeric fields
        val = payload.value
        if payload.field in ("est_ht", "reel_ht"):
            try:
                val = float(val)
            except:
                pass
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=cell_range,
            valueInputOption="USER_ENTERED",
            body={"values": [[val]]}
        ).execute()
        return {"status": "ok", "updated": cell_range, "value": val}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Health check ──────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "ok", "service": "BudgetFlow API", "version": "1.0"}
