# Squarespace Orders Sync

Containerized Python job that:

1. Fetches pending Squarespace Commerce orders in a configurable date window (default: last 30 days).
1. Writes new order rows to a local CSV export.
1. Appends only new orders to the `orders_v2` worksheet (de-duplicated by `order_id`).
1. Maps/reconciles Squarespace orders into the `members` worksheet using `squarespace:<order_id>` as the external key.

Environment variables are loaded from `.env`.

## Run In Docker

Build:

```bash
docker build -t iba-orders-sync .
```

Run:

```bash
docker run --rm \
  --env-file .env \
  -v "$(pwd)/.secrets:/app/.secrets:ro" \
  -v "$(pwd):/app/output" \
  -e OUTPUT_FILE=/app/output/iba_squarespace_orders.csv \
  iba-orders-sync
```

## Environment Variables

Required:

- `API_KEY`: Squarespace API key.

Optional:

- `STORE_ID`: Limits fetches to one Squarespace store.
- `DAYS_BACK`: Number of days to fetch (default `30`).
- `HTTP_TIMEOUT_SECONDS`: HTTP request timeout in seconds (default `30`).
- `OUTPUT_FILE`: CSV output path (default `iba_squarespace_orders.csv`).
- `GOOGLE_SHEET_ID`: Target spreadsheet ID.
- `GOOGLE_WORKSHEET`: Orders worksheet name (default `orders`; current setup uses `orders_v2`).
- `GOOGLE_MEMBERS_WORKSHEET`: Members worksheet name (default `members`).
- `GOOGLE_CREDENTIALS_FILE`: Service account JSON path.

If Google Sheets values are not configured, the script still writes CSV output.

## Current Data Behavior

- Fetch scope: pending orders created between `now - DAYS_BACK` and `now`.
- Orders de-duplication: existing `order_id` values in the orders sheet are skipped.
- Members de-duplication: existing `DATABASE` values starting with `squarespace:` are not appended again.
- Members column policy: column `A` remains blank for auto-generated sheet IDs.
- Mapping source: first line item customizations are used, with custom `Name`/`Address` preferred over billing details.
- `EXPIRES` calculation: purchase date plus computed membership years, where years are derived from the first line item using `$35.00/year` (with quantity fallback).
- Normalization:
  - Name/address/city/satellite group title-cased with acronym preservation (`TBD`, `US`, `USA`).
  - Emails lowercased.
  - State normalized to USPS-style abbreviations (including military/fallback aliases).
  - Phone normalized to US display format when possible: `(###) ###-####`.
  - Placeholder address2 values like `Apt/Suite (Optional)` are removed.

## Notes

- Orders are written with a raw JSON column, which is used to derive/reconcile `members` rows.
- Existing members rows are repaired/normalized in place when they are recognized as Squarespace-origin rows.
