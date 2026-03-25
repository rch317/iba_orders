# Squarespace Pending Orders Sync

This project fetches Squarespace orders from the last year and appends them to a Google Sheet.

Environment variables are loaded from `.env`.

Run in Docker

1. Build the image:

docker build -t iba-orders-sync .

1. Run the container (mount Google service account credentials):

docker run --rm \
  --env-file .env \
  -v "$(pwd)/.secrets:/app/.secrets:ro" \
  -v "$(pwd):/app/output" \
  -e OUTPUT_FILE=/app/output/iba_squarespace_orders.csv \
  iba-orders-sync

Notes

- Required env vars: API_KEY
- Optional env vars: STORE_ID, DAYS_BACK, OUTPUT_FILE, GOOGLE_SHEET_ID, GOOGLE_WORKSHEET, GOOGLE_MEMBERS_WORKSHEET, GOOGLE_CREDENTIALS_FILE
- If Google Sheets env vars are not set, the script still writes CSV output.
- Duplicate prevention: if Google Sheets is configured, rows for existing order_id values are skipped.
- Members mapping: rows in orders worksheet are mapped into members worksheet (deduped by ID).
