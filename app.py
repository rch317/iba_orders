import csv
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

import gspread
import requests
import us
from dotenv import load_dotenv


API_BASE_URL = "https://api.squarespace.com/1.0/commerce/orders"
DEFAULT_DAYS_BACK = 30
DEFAULT_PAGE_SIZE = 50
DEFAULT_TIMEOUT_SECONDS = 30
ORDER_ID_PATTERN = re.compile(r"^[a-f0-9]{24}$", re.IGNORECASE)
OPTIONAL_ADDRESS2_VALUES = {
    "apt/suite (optional)",
    "apt/suite optional",
    "optional",
}
MEMBERSHIP_PRICE_PER_YEAR = Decimal("35.00")
USPS_STATE_FALLBACKS = {
    "armed forces americas": "AA",
    "armed forces europe": "AE",
    "armed forces europe, middle east, canada": "AE",
    "armed forces pacific": "AP",
}
STATE_TOKEN_ALIASES = {
    "ind": "IN",
}


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    api_key: str
    store_id: str | None
    days_back: int
    output_file: str
    google_sheet_id: str | None
    google_worksheet: str
    google_members_worksheet: str
    google_credentials_file: str | None
    timeout_seconds: int


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config() -> Config:
    load_dotenv()

    days_back_raw = os.getenv("DAYS_BACK", str(DEFAULT_DAYS_BACK))
    timeout_raw = os.getenv("HTTP_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS))

    try:
        days_back = int(days_back_raw)
    except ValueError as exc:
        raise ValueError("DAYS_BACK must be an integer") from exc

    try:
        timeout_seconds = int(timeout_raw)
    except ValueError as exc:
        raise ValueError("HTTP_TIMEOUT_SECONDS must be an integer") from exc

    return Config(
        api_key=env_required("API_KEY"),
        store_id=os.getenv("STORE_ID"),
        days_back=days_back,
        output_file=os.getenv("OUTPUT_FILE", "iba_squarespace_orders.csv"),
        google_sheet_id=os.getenv("GOOGLE_SHEET_ID"),
        google_worksheet=os.getenv("GOOGLE_WORKSHEET", "orders"),
        google_members_worksheet=os.getenv("GOOGLE_MEMBERS_WORKSHEET", "members"),
        google_credentials_file=os.getenv("GOOGLE_CREDENTIALS_FILE"),
        timeout_seconds=timeout_seconds,
    )


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def add_years(dt: datetime, years: int) -> datetime:
    try:
        return dt.replace(year=dt.year + years)
    except ValueError:
        # Handle leap-day purchases by using the last valid day in February.
        return dt.replace(month=2, day=28, year=dt.year + years)


def safe_decimal(value: Any) -> str:
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        return ""


def format_customizations(customizations: list[dict[str, Any]]) -> str:
    pairs: list[str] = []
    for item in customizations:
        label = str(item.get("label", "")).strip()
        value = str(item.get("value", "")).strip()
        if label and value:
            pairs.append(f"{label}: {value}")
    return " | ".join(pairs)


def fetch_recent_orders(config: Config) -> list[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {config.api_key}",
            "Accept": "application/json",
            "User-Agent": "iba-orders-sync/1.0",
        }
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=config.days_back)
    params: dict[str, Any] = {
        "createdAfter": iso_utc(cutoff),
        "createdBefore": iso_utc(datetime.now(timezone.utc)),
        "limit": DEFAULT_PAGE_SIZE,
        "fulfillmentStatus": "PENDING",
    }
    if config.store_id:
        params["storeId"] = config.store_id

    orders: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        if cursor:
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        response = session.get(API_BASE_URL, params=params, timeout=config.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        page_orders = payload.get("result", [])
        if not isinstance(page_orders, list):
            raise RuntimeError("Squarespace response has invalid 'result' format")

        orders.extend(page_orders)

        pagination = payload.get("pagination", {})
        next_cursor = pagination.get("nextPageCursor") or payload.get("nextPageCursor") or payload.get("nextCursor")
        if not next_cursor:
            break
        cursor = str(next_cursor)

    # Keep local filtering as a safety net in case API-side filters are changed.
    filtered: list[dict[str, Any]] = []
    for order in orders:
        status = str(order.get("fulfillmentStatus", "")).strip().upper()
        if status != "PENDING":
            continue

        created_on = parse_timestamp(order.get("createdOn"))
        modified_on = parse_timestamp(order.get("modifiedOn"))

        if created_on and created_on >= cutoff:
            filtered.append(order)
            continue
        if modified_on and modified_on >= cutoff:
            filtered.append(order)

    return filtered


def order_rows(orders: list[dict[str, Any]]) -> list[list[str]]:
    pulled_at = iso_utc(datetime.now(timezone.utc))
    rows: list[list[str]] = []

    for order in orders:
        line_items = order.get("lineItems") or []
        if not line_items:
            line_items = [{}]

        for line_item in line_items:
            customizations = line_item.get("customizations") or []
            row = [
                pulled_at,
                str(order.get("id", "")),
                str(order.get("orderNumber", "")),
                str(order.get("createdOn", "")),
                str(order.get("modifiedOn", "")),
                str(order.get("fulfillmentStatus", "")),
                str(order.get("customerEmail", "")),
                str(order.get("channelName", "")),
                str(line_item.get("productName", "")),
                str(line_item.get("sku", "")),
                str(line_item.get("quantity", "")),
                safe_decimal((line_item.get("unitPricePaid") or {}).get("value", "")),
                safe_decimal((order.get("grandTotal") or {}).get("value", "")),
                format_customizations(customizations),
                json.dumps(order, separators=(",", ":"), ensure_ascii=True),
            ]
            rows.append(row)

    return rows


def write_csv(rows: list[list[str]], path: str) -> None:
    header = [
        "pulled_at_utc",
        "order_id",
        "order_number",
        "created_on",
        "modified_on",
        "fulfillment_status",
        "customer_email",
        "channel_name",
        "product_name",
        "sku",
        "quantity",
        "unit_price",
        "order_total",
        "customizations",
        "raw_order_json",
    ]

    with open(path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)
        writer.writerows(rows)


def append_to_google_sheet(config: Config, rows: list[list[str]]) -> None:
    if not rows:
        logger.info("No rows to append to Google Sheets.")
        return

    if not config.google_sheet_id or not config.google_credentials_file:
        logger.info("Google Sheets configuration not present. Skipping sheet sync.")
        return

    client = gspread.service_account(filename=config.google_credentials_file)
    spreadsheet = client.open_by_key(config.google_sheet_id)
    worksheet = spreadsheet.worksheet(config.google_worksheet)
    worksheet.append_rows(rows, value_input_option="RAW")


def existing_order_ids_in_sheet(config: Config) -> set[str]:
    if not config.google_sheet_id or not config.google_credentials_file:
        return set()

    client = gspread.service_account(filename=config.google_credentials_file)
    spreadsheet = client.open_by_key(config.google_sheet_id)
    worksheet = spreadsheet.worksheet(config.google_worksheet)

    # Column 2 is order_id based on write_csv header.
    order_id_values = worksheet.col_values(2)
    if not order_id_values:
        return set()

    # Skip header-like values defensively and empty cells.
    return {
        value.strip()
        for value in order_id_values
        if value and value.strip() and value.strip().lower() != "order_id"
    }


def filter_new_rows(config: Config, rows: list[list[str]]) -> list[list[str]]:
    existing_order_ids = existing_order_ids_in_sheet(config)
    if not existing_order_ids:
        return rows

    filtered_rows: list[list[str]] = []
    for row in rows:
        order_id = row[1].strip() if len(row) > 1 else ""
        if not order_id:
            filtered_rows.append(row)
            continue
        if order_id in existing_order_ids:
            continue
        filtered_rows.append(row)

    return filtered_rows


def customization_map(line_item: dict[str, Any]) -> dict[str, str]:
    customizations = line_item.get("customizations") or []
    result: dict[str, str] = {}
    for item in customizations:
        label = str(item.get("label", "")).strip()
        value = str(item.get("value", "")).strip()
        if label and value:
            result[label] = value
    return result


def bool_to_yn(value: bool) -> str:
    return "Y" if value else "N"


def member_external_key(order_id: str) -> str:
    return f"squarespace:{order_id}"


def normalize_spaces(value: str) -> str:
    return " ".join(value.split())


def clean_optional_address2(value: str) -> str:
    normalized = normalize_spaces(value)
    if normalized.lower() in OPTIONAL_ADDRESS2_VALUES:
        return ""
    return normalized


def normalize_state_abbreviation(value: str) -> str:
    normalized = normalize_spaces(value)
    if not normalized:
        return ""

    normalized = re.sub(r"\b(?:United States|US|USA)\b", "", normalized, flags=re.IGNORECASE)
    normalized = normalize_spaces(normalized)
    if not normalized:
        return ""

    state = us.states.lookup(normalized)
    if state is not None:
        return state.abbr

    if normalized.lower() in USPS_STATE_FALLBACKS:
        return USPS_STATE_FALLBACKS[normalized.lower()]

    if normalized.lower() in STATE_TOKEN_ALIASES:
        return STATE_TOKEN_ALIASES[normalized.lower()]

    return normalized.upper()


def normalize_phone(value: str) -> str:
    raw = normalize_spaces(value)
    if not raw:
        return ""

    digits = "".join(ch for ch in raw if ch.isdigit())

    # US country code prefix.
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"

    if len(digits) == 7:
        return f"{digits[0:3]}-{digits[3:7]}"

    return raw


def title_case(value: str) -> str:
    normalized = clean_optional_address2(value)
    if not normalized:
        return ""
    titled = normalized.title()
    titled = re.sub(r"\bTbd\b", "TBD", titled)
    titled = re.sub(r"\bUs\b", "US", titled)
    titled = re.sub(r"\bUsa\b", "USA", titled)
    return titled


def parse_custom_address(address_value: str) -> dict[str, str]:
    value = normalize_spaces(address_value)
    if not value:
        return {}

    parts = [part.strip() for part in value.split(",") if part.strip()]
    if not parts:
        return {}

    parsed: dict[str, str] = {
        "address1": parts[0],
        "address2": "",
        "city": "",
        "state": "",
        "postalCode": "",
    }

    zip_index = -1
    zip_value = ""
    for idx in range(len(parts) - 1, -1, -1):
        zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\b", parts[idx])
        if zip_match:
            zip_index = idx
            zip_value = zip_match.group(1)
            break

    parsed["postalCode"] = zip_value

    state_index = -1
    state_value = ""
    if zip_index >= 0:
        zip_part = parts[zip_index]
        before_zip = normalize_spaces(zip_part[: zip_part.find(zip_value)]).strip() if zip_value else ""
        if before_zip:
            state_value = before_zip
            state_index = zip_index
        elif zip_index - 1 >= 1:
            state_candidate = normalize_spaces(parts[zip_index - 1])
            state_candidate = re.sub(r"\b(?:United States|US|USA)\b", "", state_candidate, flags=re.IGNORECASE)
            state_candidate = normalize_spaces(state_candidate)
            if state_candidate:
                state_value = state_candidate
                state_index = zip_index - 1

    parsed["state"] = normalize_state_abbreviation(state_value)

    city_index = state_index - 1 if state_index > 1 else (zip_index - 1 if zip_index > 1 else -1)
    if city_index >= 1:
        parsed["city"] = parts[city_index]

    address_parts_end = city_index if city_index >= 1 else (zip_index if zip_index >= 1 else len(parts))
    street_parts = parts[:address_parts_end]
    if street_parts:
        parsed["address1"] = street_parts[0]
    if len(street_parts) > 1:
        parsed["address2"] = clean_optional_address2(", ".join(street_parts[1:]))

    return parsed


def normalize_members_row_case(row: list[str]) -> list[str]:
    normalized = row[:29] + [""] * max(0, 29 - len(row))
    normalized = [cell.strip() for cell in normalized]

    # Keep these human-readable fields consistently title-cased.
    for idx in (1, 6, 9, 12, 13, 14, 23):
        normalized[idx] = title_case(normalized[idx])

    normalized[13] = clean_optional_address2(normalized[13])

    # Normalize booleans and canonical formats.
    for idx in (3, 5, 24):
        normalized[idx] = normalized[idx].upper()

    normalized[15] = normalize_state_abbreviation(normalized[15])
    normalized[21] = normalized[21].lower()  # Email
    normalized[17] = normalize_phone(normalized[17])

    if normalized[27].lower().startswith("squarespace:"):
        prefix, _, suffix = normalized[27].partition(":")
        normalized[27] = f"{prefix.lower()}:{suffix.lower()}"

    return normalized


def parse_positive_int(value: Any, default: int = 1) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def membership_years_for_line_item(line_item: dict[str, Any]) -> int:
    quantity = parse_positive_int(line_item.get("quantity", 1), default=1)
    unit_price = safe_decimal((line_item.get("unitPricePaid") or {}).get("value", ""))

    if unit_price:
        try:
            total_paid = Decimal(unit_price) * Decimal(quantity)
            computed_years = int((total_paid / MEMBERSHIP_PRICE_PER_YEAR).to_integral_value(rounding=ROUND_HALF_UP))
            if computed_years > 0:
                return computed_years
        except (InvalidOperation, ValueError):
            pass

    return quantity


MANAGED_MEMBER_COLUMN_INDEXES = (0, 1, 2, 3, 5, 6, 9, 12, 13, 14, 15, 16, 17, 21, 23, 24, 26, 27)


def merge_member_row(existing_row: list[str], desired_row: list[str]) -> list[str]:
    merged = (existing_row[:29] + [""] * max(0, 29 - len(existing_row)))[:29]
    desired = normalize_members_row_case(desired_row)
    for idx in MANAGED_MEMBER_COLUMN_INDEXES:
        merged[idx] = desired[idx]
    return normalize_members_row_case(merged)


def members_row_from_order(order: dict[str, Any]) -> list[str]:
    row = [""] * 29

    order_id = str(order.get("id", "")).strip()
    created_on = parse_timestamp(order.get("createdOn"))

    line_items = order.get("lineItems") or []
    line_item = line_items[0] if line_items else {}
    product_name = str(line_item.get("productName", ""))

    billing = order.get("billingAddress") or {}
    custom = customization_map(line_item)

    # For memberships purchased by someone else, custom Name/Address can represent
    # the actual member and should take precedence over billing fields.
    full_name = custom.get("Name", "").strip()
    first_name = ""
    last_name = ""
    if full_name:
        parts = full_name.split()
        if parts:
            first_name = parts[0]
            last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    if not first_name and not last_name:
        first_name = str(billing.get("firstName", "")).strip()
        last_name = str(billing.get("lastName", "")).strip()

    public_list_pref = custom.get("Public Membership List", "")
    newsletter_pref = custom.get("Newsletter via Email", "")
    new_or_renewing = custom.get("New Member or Renewing", "")
    satellite_group = custom.get("Home Satellite Group", "")
    custom_address = parse_custom_address(custom.get("Address", ""))

    row[0] = ""
    row[1] = new_or_renewing
    membership_years = membership_years_for_line_item(line_item)

    row[2] = (
        add_years(created_on, membership_years).date().isoformat()
        if created_on
        else ""
    )
    row[3] = bool_to_yn("not" not in public_list_pref.lower())
    row[4] = ""
    row[5] = bool_to_yn(bool(newsletter_pref))
    row[6] = last_name
    row[9] = first_name
    row[12] = custom_address.get("address1") or str(billing.get("address1", "")).strip()
    row[13] = custom_address.get("address2") or clean_optional_address2(str(billing.get("address2", "") or ""))
    row[14] = custom_address.get("city") or str(billing.get("city", "")).strip()
    row[15] = custom_address.get("state") or str(billing.get("state", "")).strip()
    row[16] = custom_address.get("postalCode") or str(billing.get("postalCode", "")).strip()
    row[17] = normalize_phone(custom.get("Phone", "") or str(billing.get("phone", "")).strip())
    row[21] = custom.get("Email", "") or str(order.get("customerEmail", "")).strip()
    row[23] = satellite_group
    row[24] = bool_to_yn("recurring" in product_name.lower())
    row[26] = created_on.date().isoformat() if created_on else ""
    row[27] = member_external_key(order_id)

    return normalize_members_row_case(row)


def sync_members_from_orders_sheet(config: Config) -> int:
    if not config.google_sheet_id or not config.google_credentials_file:
        logger.info("Google Sheets configuration not present. Skipping members sync.")
        return 0

    client = gspread.service_account(filename=config.google_credentials_file)
    spreadsheet = client.open_by_key(config.google_sheet_id)
    orders_ws = spreadsheet.worksheet(config.google_worksheet)
    members_ws = spreadsheet.worksheet(config.google_members_worksheet)

    order_rows = orders_ws.get_all_values()
    if not order_rows:
        logger.info("Orders sheet has no data rows. Skipping members sync.")
        return 0

    orders_by_id: dict[str, dict[str, Any]] = {}
    for source_row in order_rows:
        if len(source_row) < 15:
            continue
        order_id_cell = source_row[1].strip().lower() if len(source_row) > 1 else ""
        if order_id_cell == "order_id":
            continue
        raw_json = source_row[14].strip()
        if not raw_json:
            continue
        try:
            order = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        order_id = str(order.get("id", "")).strip()
        if order_id:
            orders_by_id[order_id] = order

    members_values = members_ws.get_all_values()

    # Repair rows that were previously shifted right by 26 columns.
    repairs: list[tuple[int, list[str]]] = []
    for row_index, row in enumerate(members_values, start=1):
        padded = row + [""] * max(0, 54 - len(row))
        left_id = padded[0].strip() if len(padded) > 0 else ""
        shifted_id = padded[26].strip() if len(padded) > 26 else ""
        if left_id:
            continue
        if not ORDER_ID_PATTERN.match(shifted_id):
            continue
        repaired = normalize_members_row_case((padded[26:54] + [""])[:29])
        repairs.append((row_index, repaired))

    # Normalize existing rows to avoid writing order IDs into auto-generated column A
    # and reconcile managed fields from the latest order data.
    for row_index, row in enumerate(members_values, start=1):
        padded = row + [""] * max(0, 54 - len(row))
        left_id = padded[0].strip() if len(padded) > 0 else ""
        shifted_id = padded[26].strip() if len(padded) > 26 else ""
        database_value = padded[27].strip() if len(padded) > 27 else ""

        existing_order_id = ""
        if database_value.lower().startswith("squarespace:"):
            existing_order_id = database_value.split(":", 1)[1].strip().lower()
        elif ORDER_ID_PATTERN.match(left_id):
            existing_order_id = left_id.lower()
        elif ORDER_ID_PATTERN.match(shifted_id):
            existing_order_id = shifted_id.lower()

        if not existing_order_id:
            continue

        repaired = normalize_members_row_case(padded[:29])
        repaired[0] = ""
        repaired[27] = member_external_key(existing_order_id)

        order = orders_by_id.get(existing_order_id)
        if order:
            repaired = merge_member_row(repaired, members_row_from_order(order))

        repairs.append((row_index, repaired))

    # Normalize case for all existing Squarespace-origin rows.
    for row_index, row in enumerate(members_values, start=1):
        padded = row + [""] * max(0, 54 - len(row))
        current = padded[:29]
        database_value = current[27].strip() if len(current) > 27 else ""
        if not database_value.lower().startswith("squarespace:"):
            continue
        normalized = normalize_members_row_case(current)
        if normalized != current:
            repairs.append((row_index, normalized))

    deduped_repairs: dict[int, list[str]] = {}
    for row_index, repaired in repairs:
        deduped_repairs[row_index] = repaired

    if deduped_repairs:
        repair_requests = [
            {"range": f"A{row_index}:AC{row_index}", "values": [repaired]}
            for row_index, repaired in deduped_repairs.items()
        ]
        members_ws.batch_update(repair_requests, value_input_option="RAW")

    if deduped_repairs:
        members_values = members_ws.get_all_values()

    existing_member_ids: set[str] = set()
    for row in members_values:
        padded = row + [""] * max(0, 54 - len(row))
        database_value = padded[27].strip() if len(padded) > 27 else ""
        if database_value.lower().startswith("squarespace:"):
            existing_member_ids.add(database_value.split(":", 1)[1].strip())
            continue
        # Backward compatibility with older rows that stored order ID in A or shifted AA.
        for idx in (0, 26):
            if idx >= len(padded):
                continue
            candidate = padded[idx].strip()
            if ORDER_ID_PATTERN.match(candidate):
                existing_member_ids.add(candidate)

    mapped_rows: list[list[str]] = []
    seen_ids: set[str] = set()

    for order_id, order in orders_by_id.items():
        if not order_id:
            continue
        if order_id in existing_member_ids or order_id in seen_ids:
            continue

        mapped_rows.append(members_row_from_order(order))
        seen_ids.add(order_id)

    if not mapped_rows:
        logger.info("No new rows to append to members worksheet.")
        return 0

    start_row = len(members_values) + 1
    end_row = start_row + len(mapped_rows) - 1
    members_ws.batch_update(
        [{"range": f"A{start_row}:AC{end_row}", "values": mapped_rows}],
        value_input_option="RAW",
    )
    return len(mapped_rows)


def main() -> None:
    config = load_config()
    orders = fetch_recent_orders(config)
    rows = order_rows(orders)
    new_rows = filter_new_rows(config, rows)

    write_csv(new_rows, config.output_file)
    append_to_google_sheet(config, new_rows)

    logger.info("Fetched %s orders from the last %s days.", len(orders), config.days_back)
    logger.info("Filtered to %s new rows after de-duplication.", len(new_rows))
    logger.info("Wrote %s rows to %s.", len(new_rows), config.output_file)
    if config.google_sheet_id and config.google_credentials_file:
        logger.info("Appended %s rows to Google Sheet worksheet '%s'.", len(new_rows), config.google_worksheet)

    members_added = sync_members_from_orders_sheet(config)
    if config.google_sheet_id and config.google_credentials_file:
        logger.info(
            "Appended %s mapped rows to Google Sheet worksheet '%s'.",
            members_added,
            config.google_members_worksheet,
        )


if __name__ == "__main__":
    main()
