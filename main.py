from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import re
import io
import json
import unicodedata
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ────────────────────────────────────────────────────────────────
SHEET_ID = os.environ.get("SHEET_ID", "1mc_ofkHODCLuhFlV4w8-D3c8KeMYPkIooYpQh0pqLAM")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

DEPT_TABS = [
    "REPERAGES", "VHR", "PRODUCTION", "CATERING",
    "BAR", "ARTISTIQUE", "TECHNIQUE", "SCENO",
    "PARTENARIAT", "COMMUNICATION"
]

RAW_TAB       = "QONTO_RAW"
UNMATCHED_TAB = "QONTO_UNMATCHED"

COL = {
    "section": 0, "ligne": 1, "observations": 2, "typ": 3, "tva": 4,
    "est_ht": 5, "est_ttc": 6, "rh": 7, "reel_ht": 8, "reel_ttc": 9,
    "ecart": 10, "statut": 11
}

FIELD_TO_COL = {"est_ht": "F", "reel_ht": "I", "statut": "L", "observations": "C", "ligne": "B", "tva": "E", "typ": "D"}

QC = {
    "cat": 0, "sous_cat": 1, "fourn": 2, "note": 3, "date": 4,
    "ht": 5, "ttc": 6, "ref": 7, "methode": 8, "init": 9, "carte": 10
}

POLE_MAP = {
    "PRODUCTION":   "PRODUCTION",
    "TECHNIQUE":    "TECHNIQUE",
    "ARTISTIQUE":   "ARTISTIQUE",
    "VHR":          "VHR",
    "CATERING":     "CATERING",
    "RESTAURATION": "CATERING",
    "AMENAGEMENT":  "SCENO",
    "SCENO":        "SCENO",
    "COMMUNICATION":"COMMUNICATION",
    "PARTENARIAT":  "PARTENARIAT",
    "BAR":          "BAR",
}

SKIP_CATEGORIES  = {"HUMAIN", "PERFORMANCE", "DIVERS", "TRANSPORT", "LOGEMENT"}
REVENUE_CATEGORY = "BILLETERIE"

RAW_HEADERS = [
    "Catégorie de trésorerie", "Sous-catégorie de trésorerie",
    "Nom de la contrepartie", "Note", "Date de l'opération (local)",
    "Montant total (HT)", "Montant total (TTC)", "Référence",
    "Méthode de paiement", "Initiateur", "Nom de la carte",
]

UNMATCHED_HEADERS = [
    "Catégorie", "Sous-catégorie", "Fournisseur", "Note", "Date",
    "Montant HT", "Montant TTC", "Référence",
    "Raison non-match", "Tab suggéré", "Ligne suggérée",
    "Statut", "Tab assigné", "Ligne assignée",
]

# ── Credentials ───────────────────────────────────────────────────────────
def get_creds_dict():
    raw = os.environ.get("GOOGLE_CREDENTIALS", "").strip().strip("'\"")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        try:
            fixed = re.sub(
                r'("private_key"\s*:\s*")([\s\S]*?)(?="(?:\s*,|\s*}))',
                lambda m: m.group(1) + m.group(2).replace('\n', '\\n'),
                raw
            )
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
    private_key  = os.environ.get("PRIVATE_KEY", "").replace("\\n", "\n")
    client_email = os.environ.get("CLIENT_EMAIL", "")
    if private_key and client_email:
        return {
            "type": "service_account",
            "project_id": os.environ.get("PROJECT_ID", "budgetflow-489703"),
            "private_key_id": os.environ.get("PRIVATE_KEY_ID", ""),
            "private_key": private_key,
            "client_email": client_email,
            "client_id": os.environ.get("CLIENT_ID", ""),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    return None

def get_sheets_service():
    creds_dict = get_creds_dict()
    if not creds_dict:
        raise HTTPException(status_code=500, detail="No valid credentials found.")
    try:
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds).spreadsheets()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Credentials error: {str(e)}")

# ── Helpers ───────────────────────────────────────────────────────────────
def safe_float(v):
    try:
        s = str(v).replace(" ", "").replace("\u202f", "").replace("\u2009", "").replace(",", ".").replace("\u2212", "-")
        return float(s) if s not in ("", "—") else 0.0
    except:
        return 0.0

def normalize_str(s):
    if not s:
        return ""
    s = str(s).upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fuzzy_match(a: str, b: str) -> bool:
    words_a = [w for w in a.split() if len(w) > 2]
    words_b = [w for w in b.split() if len(w) > 2]
    if not words_b:
        return False
    matches = sum(1 for w in words_b if any(w in wa or wa in w for wa in words_a))
    return (matches / len(words_b)) >= 0.6

def extract_note_desc(note: str):
    if not note:
        return None
    parts = note.split(" - ")
    if len(parts) >= 3:
        return " - ".join(parts[2:]).strip()
    slash = note.split(" / ")
    if len(slash) >= 3:
        return " / ".join(slash[2:]).strip()
    return None

def is_total_row(section: str, ligne: str) -> bool:
    return "TOTAL" in (section + " " + ligne).upper()

def parse_dept_rows(values: list, tab: str) -> list:
    rows = []
    current_section = ""
    for i, row in enumerate(values):
        if i < 2:
            continue
        row = list(row) + [""] * (12 - len(row))
        section_val = str(row[COL["section"]]).strip()
        ligne_val   = str(row[COL["ligne"]]).strip()
        if section_val and not ligne_val:
            current_section = section_val
            continue
        if "TOTAL" in ligne_val.upper():
            continue
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
            "_row":         i + 1
        })
    return rows

# ── App ───────────────────────────────────────────────────────────────────
app = FastAPI(title="BudgetFlow API", version="1.3")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/")
def health():
    creds = get_creds_dict()
    return {
        "status": "ok",
        "service": "BudgetFlow API",
        "version": "1.3",
        "endpoints": ["/api/budget", "/api/unmatched", "/api/assign", "/api/import-qonto"],
        "credentials_loaded": creds is not None,
        "credential_method": "json" if os.environ.get("GOOGLE_CREDENTIALS") else "individual_vars"
    }

@app.get("/api/budget")
def get_budget():
    try:
        svc = get_sheets_service()
        result = {}
        for tab in DEPT_TABS:
            resp = svc.values().get(
                spreadsheetId=SHEET_ID,
                range=f"{tab}!A:L",
                valueRenderOption="UNFORMATTED_VALUE"
            ).execute()
            result[tab] = parse_dept_rows(resp.get("values", []), tab)
        return {"status": "ok", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/budget/{dept}")
def get_dept(dept: str):
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
        return {"status": "ok", "dept": tab, "data": parse_dept_rows(resp.get("values", []), tab)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class UpdateCell(BaseModel):
    dept:  str
    row:   int
    field: str
    value: str

@app.post("/api/budget/update")
def update_cell(payload: UpdateCell):
    tab = payload.dept.upper()
    if tab not in DEPT_TABS:
        raise HTTPException(status_code=404, detail=f"Dept '{tab}' not found")
    col = FIELD_TO_COL.get(payload.field)
    if not col:
        raise HTTPException(status_code=400, detail=f"Field '{payload.field}' not writable")
    try:
        svc = get_sheets_service()
        cell_range = f"{tab}!{col}{payload.row}"
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class AddLigne(BaseModel):
    dept:    str
    section: str
    ligne:   str
    obs:     str = ""
    typ:     str = ""
    tva:     str = ""
    est_ht:  float = 0.0
    statut:  str = "ESTIMATION"

@app.post("/api/budget/add-ligne")
def add_ligne(payload: AddLigne):
    tab = payload.dept.upper()
    if tab not in DEPT_TABS:
        raise HTTPException(status_code=404, detail=f"Dept '{tab}' not found")
    try:
        svc = get_sheets_service()
        # Read current sheet to find where to insert (after last row of the section)
        resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{tab}!A:L",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        values = resp.get("values", [])

        # Find the last row of the target section
        insert_after = len(values)  # default: append at end
        in_section = False
        for i, row in enumerate(values):
            row_p = list(row) + [""] * 12
            sec_val   = str(row_p[COL["section"]]).strip().upper()
            ligne_val = str(row_p[COL["ligne"]]).strip().upper()
            target_sec = payload.section.strip().upper()

            if sec_val == target_sec and not ligne_val:
                in_section = True
            if in_section:
                # Stop at TOTAL row or next section header
                if "TOTAL" in ligne_val or ("TOTAL" in sec_val and not ligne_val and sec_val != target_sec):
                    insert_after = i  # insert before this row
                    break
                # Check if we've moved to a new section
                if sec_val and sec_val != target_sec and not ligne_val and i > 2:
                    insert_after = i
                    break
                insert_after = i + 1

        # Build the new row (12 cols: A-L)
        new_row = [""] * 12
        new_row[COL["section"]]      = ""  # section is a header row, ligne rows leave it blank
        new_row[COL["ligne"]]        = payload.ligne
        new_row[COL["observations"]] = payload.obs
        new_row[COL["typ"]]          = payload.typ
        new_row[COL["tva"]]          = payload.tva
        new_row[COL["est_ht"]]       = payload.est_ht if payload.est_ht else ""
        new_row[COL["reel_ht"]]      = 0
        new_row[COL["reel_ttc"]]     = 0
        new_row[COL["statut"]]       = payload.statut

        # Get sheet ID for insert
        sheet_meta = svc.get(spreadsheetId=SHEET_ID).execute()
        sheet_id = next(
            (s["properties"]["sheetId"] for s in sheet_meta["sheets"]
             if s["properties"]["title"] == tab), None
        )
        if sheet_id is None:
            raise HTTPException(status_code=404, detail=f"Sheet '{tab}' not found")

        # Insert a blank row at insert_after position
        svc.batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"insertDimension": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS",
                          "startIndex": insert_after, "endIndex": insert_after + 1},
                "inheritFromBefore": True
            }}]}
        ).execute()

        # Write the new row data
        sheet_row = insert_after + 1  # 1-based
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=f"{tab}!A{sheet_row}:L{sheet_row}",
            valueInputOption="USER_ENTERED",
            body={"values": [new_row]}
        ).execute()

        return {"status": "ok", "inserted_row": sheet_row, "ligne": payload.ligne, "dept": tab}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── QONTO_UNMATCHED ────────────────────────────────────────────────────────
UNMATCHED_COLS = {"date": 0, "fourn": 1, "note": 2, "cat": 3, "ht": 4, "ttc": 5, "ref": 6, "init": 7}

def parse_unmatched_rows(values: list) -> list:
    rows = []
    for i, row in enumerate(values):
        if i == 0:
            continue
        row = list(row) + [""] * (8 - len(row))
        if not any(str(c).strip() for c in row):
            continue
        rows.append({
            "idx":   i,
            "date":  str(row[UNMATCHED_COLS["date"]]).strip(),
            "fourn": str(row[UNMATCHED_COLS["fourn"]]).strip(),
            "note":  str(row[UNMATCHED_COLS["note"]]).strip(),
            "cat":   str(row[UNMATCHED_COLS["cat"]]).strip(),
            "ht":    safe_float(row[UNMATCHED_COLS["ht"]]),
            "ttc":   safe_float(row[UNMATCHED_COLS["ttc"]]),
            "ref":   str(row[UNMATCHED_COLS["ref"]]).strip(),
            "init":  str(row[UNMATCHED_COLS["init"]]).strip(),
        })
    return rows

@app.get("/api/unmatched")
def get_unmatched():
    try:
        svc = get_sheets_service()
        resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{UNMATCHED_TAB}!A:H",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        rows = parse_unmatched_rows(resp.get("values", []))
        return {"status": "ok", "count": len(rows), "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/qonto-raw")
def get_qonto_raw():
    """Return all rows from QONTO_RAW tab for the Relevé Qonto dashboard tab."""
    try:
        svc = get_sheets_service()
        resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{RAW_TAB}!A:K",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        values = resp.get("values", [])
        if len(values) < 2:
            return {"status": "ok", "count": 0, "data": []}
        rows = []
        for i, row in enumerate(values):
            if i == 0:
                continue  # skip header
            row = list(row) + [""] * (11 - len(row))
            if not any(str(c).strip() for c in row):
                continue
            rows.append({
                "cat":   str(row[QC["cat"]]).strip(),
                "fourn": str(row[QC["fourn"]]).strip(),
                "note":  str(row[QC["note"]]).strip(),
                "date":  str(row[QC["date"]]).strip(),
                "ht":    safe_float(row[QC["ht"]]),
                "ttc":   safe_float(row[QC["ttc"]]),
                "ref":   str(row[QC["ref"]]).strip(),
                "init":  str(row[QC["init"]]).strip(),
            })
        return {"status": "ok", "count": len(rows), "data": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── MANUAL ASSIGN ──────────────────────────────────────────────────────────
class AssignPayload(BaseModel):
    unmatched_idx: int
    dept:          str
    ligne:         str

@app.post("/api/assign")
def assign_transaction(payload: AssignPayload):
    tab = payload.dept.upper()
    if tab not in DEPT_TABS:
        raise HTTPException(status_code=404, detail=f"Dept '{tab}' not found")
    try:
        svc = get_sheets_service()
        unmatched_resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{UNMATCHED_TAB}!A:H",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        unmatched_rows = unmatched_resp.get("values", [])
        if payload.unmatched_idx >= len(unmatched_rows):
            raise HTTPException(status_code=404, detail="Unmatched row not found")
        tx_row = list(unmatched_rows[payload.unmatched_idx]) + [""] * 8
        ttc_val = safe_float(tx_row[UNMATCHED_COLS["ttc"]])

        dept_resp = svc.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{tab}!A:L",
            valueRenderOption="UNFORMATTED_VALUE"
        ).execute()
        dept_rows = dept_resp.get("values", [])
        target_row_num = None
        current_reel = 0.0
        for i, row in enumerate(dept_rows):
            row = list(row) + [""] * 12
            if str(row[COL["ligne"]]).strip() == payload.ligne.strip():
                target_row_num = i + 1
                current_reel = safe_float(row[COL["reel_ttc"]])
                break

        if target_row_num is None:
            raise HTTPException(status_code=404, detail=f"Ligne '{payload.ligne}' not found in {tab}")

        new_reel = current_reel + abs(ttc_val)
        svc.values().update(
            spreadsheetId=SHEET_ID,
            range=f"{tab}!J{target_row_num}",
            valueInputOption="USER_ENTERED",
            body={"values": [[new_reel]]}
        ).execute()

        sheet_meta = svc.get(spreadsheetId=SHEET_ID).execute()
        unmatched_sheet_id = next(
            (s["properties"]["sheetId"] for s in sheet_meta["sheets"]
             if s["properties"]["title"] == UNMATCHED_TAB), None
        )
        if unmatched_sheet_id is not None:
            svc.batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"deleteDimension": {"range": {
                    "sheetId": unmatched_sheet_id, "dimension": "ROWS",
                    "startIndex": payload.unmatched_idx,
                    "endIndex": payload.unmatched_idx + 1
                }}}]}
            ).execute()

        return {"status": "ok", "assigned": payload.ligne, "dept": tab,
                "ttc_added": abs(ttc_val), "new_reel_ttc": new_reel}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════════════
# QONTO IMPORT — file upload directly to Railway (bypasses Apps Script 50KB limit)
# ══════════════════════════════════════════════════════════════════════════

def parse_qonto_file(file_bytes: bytes, filename: str) -> list:
    import csv
    fname = filename.lower()

    if fname.endswith(".csv") or fname.endswith(".tsv"):
        text = file_bytes.decode("utf-8-sig", errors="replace")
        sep  = "\t" if "\t" in text.split("\n")[0] else ","
        reader = csv.reader(io.StringIO(text), delimiter=sep)
        all_rows = list(reader)
        return [list(map(str, r)) for r in all_rows[1:] if any(c.strip() for c in r)]

    if fname.endswith(".xls"):
        try:
            import xlrd
            # Try utf-8 first, fall back to latin-1 if needed
            for enc in ('utf-8', 'latin-1', 'cp1252'):
                try:
                    wb = xlrd.open_workbook(file_contents=file_bytes, encoding_override=enc)
                    ws = wb.sheet_by_index(0)
                    # Quick check: if first row has mojibake, try next encoding
                    sample = ' '.join(str(ws.cell(0,j).value) for j in range(min(ws.ncols,5)))
                    if 'Ã' in sample or '\x83' in sample:
                        continue
                    break
                except Exception:
                    continue
            ws = wb.sheet_by_index(0)
            rows = []
            for i in range(1, ws.nrows):
                row = []
                for j in range(ws.ncols):
                    cell = ws.cell(i, j)
                    if cell.ctype == xlrd.XL_CELL_DATE:
                        import datetime
                        dt = xlrd.xldate_as_datetime(cell.value, wb.datemode)
                        row.append(dt.strftime("%Y/%m/%d"))
                    elif cell.ctype == xlrd.XL_CELL_NUMBER:
                        v = cell.value
                        row.append(str(int(v)) if v == int(v) else str(v))
                    else:
                        val = str(cell.value).strip()
                        # Fix mojibake if still present: re-encode as latin-1, decode as utf-8
                        try:
                            fixed = val.encode('latin-1').decode('utf-8')
                            val = fixed
                        except (UnicodeEncodeError, UnicodeDecodeError):
                            pass
                        row.append(val)
                if any(c.strip() for c in row):
                    rows.append(row)
            return rows
        except ImportError:
            pass  # fall through to openpyxl

    # xlsx or xls fallback
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        rows = []
        first = True
        for row in ws.iter_rows(values_only=True):
            if first:
                first = False
                continue
            str_row = [str(c) if c is not None else "" for c in row]
            if any(c.strip() for c in str_row):
                rows.append(str_row)
        return rows
    except ImportError:
        raise HTTPException(status_code=500, detail="No XLS parser available on server. Install xlrd or openpyxl.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")


def find_budget_line(dept_vals: list, note: str, fourn: str, ref: str):
    candidates = []
    for i, row in enumerate(dept_vals):
        row = list(row) + [""] * 12
        ligne   = str(row[COL["ligne"]]).strip()
        section = str(row[COL["section"]]).strip()
        obs     = str(row[COL["observations"]]).strip()
        if not ligne or ligne.startswith("(ligne") or is_total_row(section, ligne):
            continue
        candidates.append({"row": i + 1, "ligne": ligne, "obs": obs})

    if not candidates:
        return None

    note_desc = extract_note_desc(note)
    if note_desc:
        nd = normalize_str(note_desc)
        for c in candidates:
            if normalize_str(c["ligne"]) == nd:
                return c
        for c in candidates:
            if fuzzy_match(normalize_str(c["ligne"]), nd):
                return c

    if fourn:
        nf = normalize_str(fourn)
        for c in candidates:
            nl = normalize_str(c["ligne"])
            if nl and nf and (nl in nf or nf in nl):
                return c

    if ref:
        for c in candidates:
            if c["obs"] and ref in c["obs"]:
                return c

    return None


@app.post("/api/import-qonto")
async def import_qonto(file: UploadFile = File(...)):
    try:
        file_bytes = await file.read()
        filename   = file.filename or "upload.xls"
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"File read error: {e}")

    try:
        raw_rows = parse_qonto_file(file_bytes, filename)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Parse error: {e}")

    if not raw_rows:
        raise HTTPException(status_code=400, detail="Aucune transaction trouvée dans le fichier.")

    svc = get_sheets_service()

    # Ensure QONTO_RAW and QONTO_UNMATCHED tabs exist
    try:
        sheet_meta = svc.get(spreadsheetId=SHEET_ID).execute()
        existing_tabs = {s["properties"]["title"]: s["properties"]["sheetId"]
                         for s in sheet_meta["sheets"]}

        tabs_to_create = [t for t in [RAW_TAB, UNMATCHED_TAB] if t not in existing_tabs]
        if tabs_to_create:
            svc.batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": t}}} for t in tabs_to_create]}
            ).execute()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tab setup error: {e}")

    # Write QONTO_RAW
    try:
        svc.values().clear(spreadsheetId=SHEET_ID, range=f"{RAW_TAB}!A:Z").execute()
        raw_write = [RAW_HEADERS] + [
            (r + [""] * len(RAW_HEADERS))[:len(RAW_HEADERS)] for r in raw_rows
        ]
        svc.values().update(
            spreadsheetId=SHEET_ID, range=f"{RAW_TAB}!A1",
            valueInputOption="RAW", body={"values": raw_write}
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing QONTO_RAW: {e}")

    # Load dept data + reset RÉEL columns
    dept_data_cache = {}
    try:
        reset_data = []
        for tab_name in DEPT_TABS:
            resp = svc.values().get(
                spreadsheetId=SHEET_ID, range=f"{tab_name}!A:L",
                valueRenderOption="UNFORMATTED_VALUE"
            ).execute()
            vals = resp.get("values", [])
            dept_data_cache[tab_name] = vals
            for i, row in enumerate(vals):
                if i < 2:
                    continue
                rp = list(row) + [""] * 12
                ligne   = str(rp[COL["ligne"]]).strip()
                section = str(rp[COL["section"]]).strip()
                if not ligne or ligne.startswith("(ligne") or is_total_row(section, ligne):
                    continue
                reset_data.append({"range": f"{tab_name}!I{i+1}:J{i+1}", "values": [[0, 0]]})

        if reset_data:
            svc.values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "RAW", "data": reset_data}
            ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resetting RÉEL: {e}")

    # Process transactions
    results = {"matched": 0, "unmatched": 0, "revenue": 0, "skipped": 0}
    unmatched_rows = []
    accumulate = {}  # (tab, row) -> [ht, ttc]

    norm_skip = {normalize_str(s) for s in SKIP_CATEGORIES}
    norm_rev  = normalize_str(REVENUE_CATEGORY)

    for raw in raw_rows:
        raw = list(raw) + [""] * max(0, 11 - len(raw))
        cat   = normalize_str(raw[QC["cat"]])
        fourn = raw[QC["fourn"]].strip()
        note  = raw[QC["note"]].strip()
        date  = raw[QC["date"]].strip()
        ref   = raw[QC["ref"]].strip()
        ttc   = safe_float(raw[QC["ttc"]])
        ht    = safe_float(raw[QC["ht"]])
        base  = (list(raw) + [""] * len(RAW_HEADERS))[:len(RAW_HEADERS)]

        if cat == norm_rev:
            results["revenue"] += 1
            continue

        if cat in norm_skip:
            unmatched_rows.append(base + [f'Catégorie "{cat}" non mappée', "", "", "PENDING", "", ""])
            results["unmatched"] += 1
            continue

        if ttc > 0:
            results["skipped"] += 1
            continue

        target_tab = POLE_MAP.get(cat)
        if not target_tab:
            unmatched_rows.append(base + [f'Catégorie "{cat}" inconnue', "", "", "PENDING", "", ""])
            results["unmatched"] += 1
            continue

        match = find_budget_line(dept_data_cache.get(target_tab, []), note, fourn, ref)

        if match:
            key = (target_tab, match["row"])
            if key not in accumulate:
                accumulate[key] = [0.0, 0.0]
            accumulate[key][0] += abs(ht)
            accumulate[key][1] += abs(ttc)
            results["matched"] += 1
        else:
            unmatched_rows.append(base + ["Ligne budgétaire non trouvée", target_tab, "", "PENDING", "", ""])
            results["unmatched"] += 1

    # Write accumulated RÉEL values
    if accumulate:
        try:
            write_data = [
                {"range": f"{tab}!I{row}:J{row}", "values": [[ht, ttc]]}
                for (tab, row), (ht, ttc) in accumulate.items()
            ]
            svc.values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "RAW", "data": write_data}
            ).execute()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error writing RÉEL values: {e}")

    # Write QONTO_UNMATCHED
    try:
        svc.values().clear(spreadsheetId=SHEET_ID, range=f"{UNMATCHED_TAB}!A:Z").execute()
        unmatched_write = [UNMATCHED_HEADERS] + [
            (list(r) + [""] * len(UNMATCHED_HEADERS))[:len(UNMATCHED_HEADERS)]
            for r in unmatched_rows
        ]
        svc.values().update(
            spreadsheetId=SHEET_ID, range=f"{UNMATCHED_TAB}!A1",
            valueInputOption="RAW", body={"values": unmatched_write}
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error writing QONTO_UNMATCHED: {e}")

    return {
        "status":       "ok",
        "transactions": len(raw_rows),
        "matched":      results["matched"],
        "unmatched":    results["unmatched"],
        "revenue":      results["revenue"],
        "skipped":      results["skipped"],
        "summary": (
            f"✅ {results['matched']} associées · "
            f"{results['unmatched']} non associées · "
            f"{results['revenue']} recettes · "
            f"{results['skipped']} ignorées"
        )
    }
