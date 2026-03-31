"""
gmail_cron.py — runs daily via GitHub Actions

What it does:
1. Connects to alkresellshoes21@gmail.com via IMAP
2. Finds unread emails from consignment@crepdogcrew.com - "Consignment Sales Report"
3. Parses the HTML table (Date, Style Name, Color, Size, Barcode, PV/Cost Price)
4. Deduplicates against existing barcodes in Supabase
5. Writes to: sales + payment_trackers + gsts
6. Generates invoices grouped by date (client: House of CDC Fashion Private Limited - Delhi)
7. Marks emails as read
"""

import imaplib
import email
import re
import os
import logging
from datetime import datetime
from email.header import decode_header

from bs4 import BeautifulSoup
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

GMAIL_ADDRESS  = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASS = os.environ["GMAIL_APP_PASSWORD"]
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]

SENDER_FILTER  = "consignment@crepdogcrew.com"
SUBJECT_FILTER = "Consignment Sales Report"
GST_MARGIN     = 500

CLIENT = {
    "id":         "fd5245f2-252d-462d-b32c-a2164d4e3028",
    "party_name": "House of CDC Fashion Private Limited - Delhi",
    "address":    "Ground Floor, Plot No. 1, Khasra No. 261, Westend Marg Garden of Five Sense Road, Saidulajaib, New Delhi-110074",
    "gstin":      "07AAGCH5076E1ZL",
    "state_name": "Delhi",
    "state_code": "07",
}

COMPANY_CODE = "ALK"

DECLARATION = (
    "1) The goods described in this invoice are pre-owned / second-hand goods supplied under the "
    "GST Margin Scheme in accordance with applicable provisions of the GST law.\n\n"
    "2) The invoice value represents the final and agreed transaction value between the parties "
    "under the Margin Scheme. Payment against this invoice shall be made in full as per the agreed "
    "terms and timelines.\n\n"
    "3) All particulars stated herein are true and correct to the best of our knowledge and belief "
    "at the time of issuance."
)


def parse_date(raw):
    raw = (raw or "").strip()
    m = re.match(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m2 = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if m2:
        return raw[:10]
    return datetime.today().strftime("%Y-%m-%d")


def parse_price(raw):
    cleaned = re.sub(r"[^\d.]", "", raw or "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def get_html_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                charset = part.get_content_charset() or "utf-8"
                return part.get_payload(decode=True).decode(charset, errors="replace")
    else:
        if msg.get_content_type() == "text/html":
            charset = msg.get_content_charset() or "utf-8"
            return msg.get_payload(decode=True).decode(charset, errors="replace")
    return None


def parse_sales_table(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    target_table = None
    for table in soup.find_all("table"):
        if "Barcode" in table.get_text():
            target_table = table
            break
    if not target_table:
        log.warning("No sales table found in email")
        return rows
    all_rows = target_table.find_all("tr")
    if len(all_rows) < 2:
        return rows
    headers = [c.get_text(strip=True).lower() for c in all_rows[0].find_all(["th", "td"])]
    col = {}
    for i, h in enumerate(headers):
        if "date" in h:                                 col["date"] = i
        elif "style" in h or "name" in h:               col["description"] = i
        elif "color" in h or "colour" in h:             col["colour"] = i
        elif "size" in h:                                col["size"] = i
        elif "barcode" in h:                             col["barcode"] = i
        elif "pv" in h or "cost" in h or "price" in h: col["price"] = i
    log.info(f"Column map: {col}")
    for row in all_rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        def cv(key, cells=cells):
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
            "price":       int(parse_price(cv("price"))),
            "sale_date":   parse_date(cv("date")),
        })
    return rows


def get_financial_year(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    if d.month >= 4:
        return f"{str(d.year)[2:]}-{str(d.year + 1)[2:]}"
    return f"{str(d.year - 1)[2:]}-{str(d.year)[2:]}"


def number_to_words(num):
    ones = ["", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"]
    teens = ["Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"]
    tens_w = ["", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"]
    if num == 0: return "Zero"
    if num < 10: return ones[num]
    if num < 20: return teens[num - 10]
    if num < 100: return tens_w[num // 10] + (" " + ones[num % 10] if num % 10 else "")
    if num < 1000: return ones[num // 100] + " Hundred" + (" and " + number_to_words(num % 100) if num % 100 else "")
    if num < 100000: return number_to_words(num // 1000) + " Thousand" + (" " + number_to_words(num % 1000) if num % 1000 else "")
    if num < 10000000: return number_to_words(num // 100000) + " Lakh" + (" " + number_to_words(num % 100000) if num % 100000 else "")
    return number_to_words(num // 10000000) + " Crore" + (" " + number_to_words(num % 10000000) if num % 10000000 else "")


def run_import():
    log.info("── Starting Gmail import job ──────────────────────────────────")

    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        mail.select("inbox")
        log.info("Gmail IMAP connected")
    except Exception as e:
        log.error(f"Gmail connection failed: {e}")
        return

    _, msg_ids_raw = mail.search(None, f'(UNSEEN FROM "{SENDER_FILTER}" SUBJECT "{SUBJECT_FILTER}")')
    msg_ids = msg_ids_raw[0].split()

    if not msg_ids:
        log.info("No new Consignment Sales Report emails found")
        mail.logout()
        return

    log.info(f"Found {len(msg_ids)} unread email(s)")

    all_rows = []
    for mid in msg_ids:
        _, data = mail.fetch(mid, "(RFC822)")
        msg = email.message_from_bytes(data[0][1])
        subject = decode_header(msg["Subject"])[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode(errors="replace")
        log.info(f"Parsing: {subject}")
        html = get_html_body(msg)
        if not html:
            continue
        parsed = parse_sales_table(html)
        log.info(f"  → {len(parsed)} rows parsed")
        all_rows.extend(parsed)
        mail.store(mid, "+FLAGS", "\\Seen")

    mail.logout()

    if not all_rows:
        log.info("No rows parsed from any email")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    existing = supabase.table("sales").select("barcode").execute()
    existing_barcodes = {r["barcode"] for r in (existing.data or [])}
    new_rows = [r for r in all_rows if r["barcode"] not in existing_barcodes]
    log.info(f"Total: {len(all_rows)} | New: {len(new_rows)} | Skipped dupes: {len(all_rows)-len(new_rows)}")

    if not new_rows:
        log.info("All barcodes already imported — nothing to do")
        return

    import_batch_id = f"GMAIL_{int(datetime.now().timestamp())}"

    supabase.table("sales").insert([{
        "import_batch_id": import_batch_id,
        "barcode": r["barcode"], "description": r["description"],
        "colour": r["colour"], "size": r["size"],
        "price": r["price"], "sale_date": r["sale_date"],
    } for r in new_rows]).execute()
    log.info(f"✅ {len(new_rows)} rows → sales")

    supabase.table("payment_trackers").insert([{
        "barcode": r["barcode"], "sale_amount": r["price"],
        "received_amount": 0, "balance": r["price"],
        "status": "unpaid", "sale_date": r["sale_date"],
    } for r in new_rows]).execute()
    log.info(f"✅ {len(new_rows)} rows → payment_trackers")

    supabase.table("gsts").insert([{
        "barcode": r["barcode"], "description": r["description"],
        "colour": r["colour"], "size": r["size"],
        "sale_amount": r["price"], "margin_taxable": GST_MARGIN,
        "purchase_amount": int(r["price"]) - GST_MARGIN,
        "gst_amount": int(round(GST_MARGIN * 0.18)),
        "month": datetime.strptime(r["sale_date"], "%Y-%m-%d").month,
        "year": datetime.strptime(r["sale_date"], "%Y-%m-%d").year,
        "status": "draft", "sale_date": r["sale_date"],
    } for r in new_rows]).execute()
    log.info(f"✅ {len(new_rows)} rows → gsts")

    # Generate invoices grouped by date
    log.info("── Starting invoice generation ────────────────────────────────")
    grouped = {}
    for r in new_rows:
        grouped.setdefault(r["sale_date"], []).append(r)

    log.info(f"Dates to invoice: {sorted(grouped.keys())}")
    invoices_created = 0

    for date, items in sorted(grouped.items()):
        try:
            financial_year = get_financial_year(date)
            existing_inv = supabase.table("invoices").select("id").eq("financial_year", financial_year).execute()
            invoice_num = len(existing_inv.data or []) + 1
            invoice_number = f"{COMPANY_CODE}/{financial_year}/{invoice_num}"
            grand_total = sum(i["price"] for i in items)

            log.info(f"Creating invoice {invoice_number} for {date} — {len(items)} items")

            inv_res = supabase.table("invoices").insert({
                "invoice_number": invoice_number, "ref_number": invoice_number,
                "invoice_date": date, "financial_year": financial_year,
                "client_id": CLIENT["id"], "client_name": CLIENT["party_name"],
                "client_address": CLIENT["address"], "client_gstin": CLIENT["gstin"],
                "client_state_name": CLIENT["state_name"], "client_state_code": CLIENT["state_code"],
                "total_quantity": len(items), "grand_total": grand_total,
                "amount_in_words": number_to_words(int(grand_total)) + " Rupees Only",
                "declaration_text": DECLARATION, "status": "draft",
            }).execute()

            invoice_id = inv_res.data[0]["id"]
            log.info(f"Invoice ID: {invoice_id}")

            supabase.table("invoice_items").insert([{
                "invoice_id": invoice_id, "row_index": idx,
                "barcode": item["barcode"],
                "description": f"{item['description']} - {item['colour']} - {item['size']}",
                "hsn_code": "640319", "quantity": 1, "unit": "Pair",
                "rate": item["price"], "amount": item["price"],
            } for idx, item in enumerate(items)]).execute()

            log.info(f"✅ Invoice {invoice_number} — {len(items)} items, Rs.{grand_total:,}")
            invoices_created += 1

        except Exception as e:
            log.error(f"Invoice creation failed for {date}: {e}", exc_info=True)

    log.info(f"── Complete: {len(new_rows)} sales + {invoices_created} invoices. Batch: {import_batch_id} ──")


if __name__ == "__main__":
    try:
        run_import()
    except Exception as e:
        log.error(f"Unhandled error: {e}", exc_info=True)
        raise
