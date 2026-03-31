"""
gmail_cron.py — runs 24/7 on Render (free background worker)

What it does every 24 hours:
1. Connects to alkresellshoes21@gmail.com via IMAP
2. Finds unread emails from consignment@crepdogcrew.com with subject "Consignment Sales Report"
3. Parses the HTML table (Date, Style Name, Color, Size, Barcode, PV/Cost Price)
4. Deduplicates against existing barcodes in Supabase
5. Writes new rows to sales + payment_trackers + gsts tables
6. Marks emails as read so they are never re-imported

Env vars required (set in Render dashboard):
  GMAIL_ADDRESS   — alkresellshoes21@gmail.com
  GMAIL_APP_PASSWORD — 16-char app password from Google Account settings
  SUPABASE_URL    — your Supabase project URL
  SUPABASE_KEY    — your Supabase service role key
"""

import imaplib
import email
import re
import os
import time
import logging
from datetime import datetime
from email.header import decode_header

from bs4 import BeautifulSoup
from supabase import create_client

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config from env vars ─────────────────────────────────────────────────────
GMAIL_ADDRESS    = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASS   = os.environ["GMAIL_APP_PASSWORD"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]

SENDER_FILTER    = "consignment@crepdogcrew.com"
SUBJECT_FILTER   = "Consignment Sales Report"
GST_MARGIN       = 500          # fixed margin per item for GST calc
SLEEP_HOURS      = 24           # how often to run


# ── Helpers ──────────────────────────────────────────────────────────────────

def parse_date(raw: str) -> str:
    """Convert DD/MM/YYYY → YYYY-MM-DD. Falls back to today."""
    raw = (raw or "").strip()
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m2 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m2:
        return raw[:10]
    return datetime.today().strftime("%Y-%m-%d")


def parse_price(raw: str) -> float:
    """Strip ₹, commas, spaces and return float."""
    cleaned = re.sub(r"[^\d.]", "", raw or "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def get_html_body(msg) -> str | None:
    """Walk a MIME message and return the first text/html part."""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                charset = part.get_content_charset() or "utf-8"
                return part.get_payload(decode=True).decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/html":
            charset = msg.get_content_charset() or "utf-8"
            return msg.get_payload(decode=True).decode(charset, errors="replace")
    return None


def parse_sales_table(html: str) -> list[dict]:
    """Extract rows from the Consignment Sales Report HTML table."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # Find the table that contains "Barcode"
    target_table = None
    for table in soup.find_all("table"):
        if "Barcode" in table.get_text():
            target_table = table
            break

    if not target_table:
        log.warning("No sales table found in email body")
        return rows

    all_rows = target_table.find_all("tr")
    if len(all_rows) < 2:
        return rows

    # Map column names → indices from header row
    header_cells = all_rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).lower() for c in header_cells]

    col = {}
    for i, h in enumerate(headers):
        if "date" in h:                              col["date"] = i
        elif "style" in h or "name" in h:            col["description"] = i
        elif "color" in h or "colour" in h:          col["colour"] = i
        elif "size" in h:                             col["size"] = i
        elif "barcode" in h:                          col["barcode"] = i
        elif "pv" in h or "cost" in h or "price" in h: col["price"] = i

    log.info(f"Column mapping: {col}")

    for row in all_rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        def cv(key):
            idx = col.get(key)
            return cells[idx].get_text(strip=True) if idx is not None and idx < len(cells) else ""

        barcode = cv("barcode").strip()
        if not barcode:
            continue

        rows.append({
            "barcode":     barcode,
            "description": cv("description"),
            "colour":      cv("colour"),
            "size":        cv("size"),
            "price":       parse_price(cv("price")),
            "sale_date":   parse_date(cv("date")),
        })

    return rows


# ── Core job ─────────────────────────────────────────────────────────────────

def run_import():
    log.info("── Starting Gmail import job ──────────────────────────")

    # 1. Connect to Gmail via IMAP
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        mail.select("inbox")
        log.info("Gmail IMAP connected")
    except Exception as e:
        log.error(f"Gmail connection failed: {e}")
        return

    # 2. Search for unread emails from the sender
    search_criteria = f'(UNSEEN FROM "{SENDER_FILTER}" SUBJECT "{SUBJECT_FILTER}")'
    _, msg_ids_raw = mail.search(None, search_criteria)
    msg_ids = msg_ids_raw[0].split()

    if not msg_ids:
        log.info("No new Consignment Sales Report emails found")
        mail.logout()
        return

    log.info(f"Found {len(msg_ids)} unread email(s)")

    # 3. Parse each email
    all_rows = []
    for mid in msg_ids:
        _, data = mail.fetch(mid, "(RFC822)")
        raw = data[0][1]
        msg = email.message_from_bytes(raw)

        subject = decode_header(msg["Subject"])[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode(errors="replace")
        log.info(f"Parsing email: {subject}")

        html = get_html_body(msg)
        if not html:
            log.warning(f"No HTML body in email {mid}")
            continue

        parsed = parse_sales_table(html)
        log.info(f"  → {len(parsed)} rows parsed")
        all_rows.extend(parsed)

        # Mark as read
        mail.store(mid, "+FLAGS", "\\Seen")

    mail.logout()

    if not all_rows:
        log.info("No rows parsed from any email")
        return

    # 4. Connect to Supabase and deduplicate
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    existing = supabase.table("sales").select("barcode").execute()
    existing_barcodes = {r["barcode"] for r in (existing.data or [])}

    new_rows = [r for r in all_rows if r["barcode"] not in existing_barcodes]
    log.info(f"Total parsed: {len(all_rows)} | New (not in DB): {len(new_rows)}")

    if not new_rows:
        log.info("All barcodes already in DB — nothing to import")
        return

    # 5. Build records for all three tables
    import_batch_id = f"GMAIL_{int(datetime.now().timestamp())}"

    sales_records = [{
        "import_batch_id": import_batch_id,
        "barcode":         r["barcode"],
        "description":     r["description"],
        "colour":          r["colour"],
        "size":            r["size"],
        "price":           r["price"],
        "sale_date":       r["sale_date"],
    } for r in new_rows]

    payment_records = [{
        "barcode":         r["barcode"],
        "sale_amount":     r["price"],
        "received_amount": 0,
        "balance":         r["price"],
        "status":          "unpaid",
        "sale_date":       r["sale_date"],
    } for r in new_rows]

    gst_records = []
    for r in new_rows:
        d = datetime.strptime(r["sale_date"], "%Y-%m-%d")
        gst_records.append({
            "barcode":         r["barcode"],
            "description":     r["description"],
            "colour":          r["colour"],
            "size":            r["size"],
            "sale_amount":     r["price"],
            "margin_taxable":  GST_MARGIN,
            "purchase_amount": r["price"] - GST_MARGIN,
            "gst_amount":      round(GST_MARGIN * 0.18, 2),
            "month":           d.month,
            "year":            d.year,
            "status":          "draft",
            "sale_date":       r["sale_date"],
        })

    # 6. Write to Supabase
    supabase.table("sales").insert(sales_records).execute()
    log.info(f"✅ Inserted {len(sales_records)} rows into sales")

    supabase.table("payment_trackers").insert(payment_records).execute()
    log.info(f"✅ Inserted {len(payment_records)} rows into payment_trackers")

    supabase.table("gsts").insert(gst_records).execute()
    log.info(f"✅ Inserted {len(gst_records)} rows into gsts")

    log.info(f"── Import complete. Batch: {import_batch_id} ──────────")


# ── Main loop ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"Gmail cron starting — will run every {SLEEP_HOURS}h")
    while True:
        try:
            run_import()
        except Exception as e:
            log.error(f"Unhandled error in run_import: {e}", exc_info=True)
        log.info(f"Sleeping {SLEEP_HOURS}h until next run...")
        time.sleep(SLEEP_HOURS * 3600)
