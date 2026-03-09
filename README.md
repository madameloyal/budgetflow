# BudgetFlow API

FastAPI backend for BudgetFlow — reads/writes the Madame Loyal budget Google Sheet.

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/api/budget` | Full budget — all dept tabs |
| GET | `/api/budget/{dept}` | Single dept (e.g. `/api/budget/PRODUCTION`) |
| POST | `/api/budget/update` | Write a cell back to the Sheet |

## POST /api/budget/update payload

```json
{
  "dept": "PRODUCTION",
  "row": 5,
  "field": "est_ht",
  "value": "45000"
}
```

Fields: `est_ht` | `reel_ht` | `statut`

## Environment variables (set in Railway)

| Variable | Value |
|---|---|
| `SHEET_ID` | `1mc_ofkHODCLuhFlV4w8-D3c8KeMYPkIooYpQh0pqLAM` |
| `GOOGLE_CREDENTIALS` | Full contents of the service account JSON (one line) |

## Deploy to Railway

1. Push this repo to GitHub
2. Connect repo to Railway
3. Set the two env vars above
4. Deploy — Railway auto-detects Python and installs requirements
