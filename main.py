from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os, json, re
from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_ID = os.environ.get("SHEET_ID", "1mc_ofkHODCLuhFlV4w8-D3c8KeMYPkIooYpQh0pqLAM")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

DEPT_TABS = ["REPERAGES","VHR","PRODUCTION","CATERING","BAR","ARTISTIQUE","TECHNIQUE","SCENO","PARTENARIAT","COMMUNICATION"]

COL = {"section":0,"ligne":1,"observations":2,"typ":3,"tva":4,"est_ht":5,"est_ttc":6,"rh":7,"reel_ht":8,"reel_ttc":9,"ecart":10,"statut":11}

FIELD_TO_COL = {"est_ht":"F","reel_ht":"I","statut":"L","observations":"C","ligne":"B"}

def get_creds_dict():
    # Method 1: full JSON string in GOOGLE_CREDENTIALS
    raw = os.environ.get("GOOGLE_CREDENTIALS","").strip().strip("'\"")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
        try:
            fixed = re.sub(
                r'("private_key"\s*:\s*")([\s\S]*?)(?="(?:\s*,|\s*}))',
                lambda m: m.group(1) + m.group(2).replace('\n','\\n'),
                raw
            )
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass

    # Method 2: individual env vars
    private_key = os.environ.get("PRIVATE_KEY","").replace("\\n","\n")
    client_email = os.environ.get("CLIENT_EMAIL","")
    if private_key and client_email:
        return {
            "type":"service_account",
            "project_id":os.environ.get("PROJECT_ID","budgetflow-489703"),
            "private_key_id":os.environ.get("PRIVATE_KEY_ID",""),
            "private_key":private_key,
            "client_email":client_email,
            "client_id":os.environ.get("CLIENT_ID",""),
            "token_uri":"https://oauth2.googleapis.com/token",
        }
    return None

def get_sheets_service():
    creds_dict = get_creds_dict()
    if not creds_dict:
        raise HTTPException(status_code=500, detail="No valid credentials. Set GOOGLE_CREDENTIALS or PRIVATE_KEY+CLIENT_EMAIL env vars.")
    try:
        creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return build("sheets","v4",credentials=creds).spreadsheets()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Credentials error: {str(e)}")

def safe_float(v):
    try:
        return float(str(v).replace(" ","").replace(",",".")) if v not in (None,"","—") else 0.0
    except:
        return 0.0

def parse_dept_rows(values, tab):
    rows = []
    current_section = ""
    for i, row in enumerate(values):
        if i < 2: continue
        row = list(row) + [""] * (12 - len(row))
        section_val = str(row[COL["section"]]).strip()
        ligne_val   = str(row[COL["ligne"]]).strip()
        if section_val and not ligne_val:
            current_section = section_val
            continue
        if "TOTAL" in ligne_val.upper(): continue
        if not ligne_val and not any(str(c).strip() for c in row[5:10]): continue
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

app = FastAPI(title="BudgetFlow API", version="1.2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET","POST"], allow_headers=["*"])

@app.get("/")
def health():
    creds = get_creds_dict()
    return {"status":"ok","service":"BudgetFlow API","version":"1.2","credentials_loaded":creds is not None}

@app.get("/api/budget")
def get_budget():
    try:
        svc = get_sheets_service()
        result = {}
        for tab in DEPT_TABS:
            resp = svc.values().get(spreadsheetId=SHEET_ID, range=f"{tab}!A:L", valueRenderOption="UNFORMATTED_VALUE").execute()
            result[tab] = parse_dept_rows(resp.get("values",[]), tab)
        return {"status":"ok","data":result}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/budget/{dept}")
def get_dept(dept: str):
    tab = dept.upper()
    if tab not in DEPT_TABS: raise HTTPException(status_code=404, detail=f"Dept '{dept}' not found")
    try:
        svc = get_sheets_service()
        resp = svc.values().get(spreadsheetId=SHEET_ID, range=f"{tab}!A:L", valueRenderOption="UNFORMATTED_VALUE").execute()
        return {"status":"ok","dept":tab,"data":parse_dept_rows(resp.get("values",[]), tab)}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

class UpdateCell(BaseModel):
    dept:  str
    row:   int
    field: str
    value: str

@app.post("/api/budget/update")
def update_cell(payload: UpdateCell):
    tab = payload.dept.upper()
    if tab not in DEPT_TABS: raise HTTPException(status_code=404, detail=f"Dept '{tab}' not found")
    col = FIELD_TO_COL.get(payload.field)
    if not col: raise HTTPException(status_code=400, detail=f"Field '{payload.field}' not writable")
    try:
        svc = get_sheets_service()
        cell_range = f"{tab}!{col}{payload.row}"
        val = payload.value
        if payload.field in ("est_ht","reel_ht"):
            try: val = float(val)
            except: pass
        svc.values().update(spreadsheetId=SHEET_ID, range=cell_range, valueInputOption="USER_ENTERED", body={"values":[[val]]}).execute()
        return {"status":"ok","updated":cell_range,"value":val}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))
