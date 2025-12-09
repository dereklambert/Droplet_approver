import imaplib
import email
from email.header import decode_header
import os
from datetime import datetime, timedelta
import zipfile


# ============================================================
# CONFIG VIA ENVIRONMENT VARIABLES
# ============================================================
# These MUST be set in the environment on the droplet:
#   export GMAIL_ADDRESS="bytesizedscripts@gmail.com"
#   export GMAIL_APP_PASSWORD="meld aoiy btvg bofa"  
#
# Optional:
#   export INVOICE_SEARCH_SUBJECT="Landscaping_Invoices"
#   export INVOICE_ATTACHMENT_DIR="/path/to/save"
#   export INVOICE_LOOKBACK_DAYS="30"
#
EMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS")
APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

SEARCH_PHRASE = os.environ.get("INVOICE_SEARCH_SUBJECT", "Landscaping_Invoices")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ATTACHMENT_DIR = os.environ.get(
    "INVOICE_ATTACHMENT_DIR",
    os.path.join(BASE_DIR, "invoice_attachments"),
)

LOOKBACK_DAYS = int(os.environ.get("INVOICE_LOOKBACK_DAYS", "30"))
# ============================================================


def connect_imap():
    """
    Connect and log in to Gmail IMAP using the app password.
    """
    if not EMAIL_ADDRESS or not APP_PASSWORD:
        raise RuntimeError(
            "GMAIL_ADDRESS and GMAIL_APP_PASSWORD must be set in the environment."
        )

    print(f"📡 Connecting to Gmail IMAP as {EMAIL_ADDRESS} ...")
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(EMAIL_ADDRESS, APP_PASSWORD)
    print("✅ IMAP login successful.")
    return mail


def build_since_date(days_back: int) -> str:
    """
    Return an IMAP SINCE date string like '26-Nov-2025'.
    """
    dt = datetime.utcnow() - timedelta(days=days_back)
    return dt.strftime("%d-%b-%Y")


def search_invoice_messages(mail):
    """
    Search INBOX for messages in the last LOOKBACK_DAYS where the
    text (subject or body) contains SEARCH_PHRASE.
    """
    mail.select("INBOX")

    since_str = build_since_date(LOOKBACK_DAYS)
    # TEXT search matches anywhere in header/body, not just subject.
    criteria = f'(SINCE "{since_str}" TEXT "{SEARCH_PHRASE}")'
    print(f"🔍 IMAP search criteria: {criteria}")

    status, data = mail.search(None, criteria)

    if status != "OK":
        print("❌ IMAP search failed:", status, data)
        return []

    msg_ids = data[0].split()
    print(
        f"Found {len(msg_ids)} matching message(s) "
        f"(TEXT contains: {SEARCH_PHRASE!r}, since {since_str})"
    )
    return msg_ids


def _decode_header_value(raw):
    """
    Helper to decode RFC2047 encoded headers (e.g. '=?UTF-8?...').
    """
    if not raw:
        return ""
    decoded_parts = decode_header(raw)
    parts = []
    for val, enc in decoded_parts:
        if isinstance(val, bytes):
            parts.append(val.decode(enc or "utf-8", errors="ignore"))
        else:
            parts.append(val)
    return "".join(parts)


def save_attachments_from_message(mail, msg_id):
    """
    Download all attachments from a single message ID into ATTACHMENT_DIR.
    Returns a list of saved file paths.
    """
    status, data = mail.fetch(msg_id, "(RFC822)")
    if status != "OK":
        print(f"❌ Failed to fetch message {msg_id!r}")
        return []

    raw_email = data[0][1]
    msg = email.message_from_bytes(raw_email)

    # Log the subject we matched on, just for sanity
    subject = _decode_header_value(msg.get("Subject", ""))
    print(f"  📧 Processing message {msg_id.decode()} with subject: {subject!r}")

    saved_files = []
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    for part in msg.walk():
        if part.get_content_disposition() != "attachment":
            continue

        filename = part.get_filename()
        if not filename:
            filename = f"attachment_{msg_id.decode()}.bin"

        filename = _decode_header_value(filename)

        filepath = os.path.join(ATTACHMENT_DIR, filename)
        payload = part.get_payload(decode=True)
        if payload is None:
            print(f"    ⚠️ Skipping empty attachment part for {filename!r}")
            continue

        with open(filepath, "wb") as f:
            f.write(payload)

        saved_files.append(filepath)
        print(f"    💾 Saved attachment: {filepath}")

    return saved_files


def extract_zip_files(filepaths, extract_root=None):
    """
    Extract all ZIP files found in 'filepaths' into 'extract_root'
    (or ATTACHMENT_DIR/invoices).
    Returns (extracted_files_list, extract_root_path).
    """
    if extract_root is None:
        extract_root = os.path.join(ATTACHMENT_DIR, "invoices")

    os.makedirs(extract_root, exist_ok=True)
    extracted_files = []

    for fp in filepaths:
        if not fp.lower().endswith(".zip"):
            continue

        print(f"🗜 Extracting ZIP: {fp}")
        try:
            with zipfile.ZipFile(fp, "r") as zf:
                zf.extractall(extract_root)
                for name in zf.namelist():
                    full = os.path.join(extract_root, name)
                    extracted_files.append(full)
                    print(f"    📄 Extracted: {full}")
        except Exception as e:
            print(f"    ❌ Error extracting {fp}: {e}")

    return extracted_files, extract_root


def download_and_extract_invoices():
    """
    High-level helper for the droplet pipeline.

    1. Connects to Gmail IMAP
    2. Searches for matching messages by TEXT
    3. Downloads attachments into ATTACHMENT_DIR
    4. Extracts any ZIPs into ATTACHMENT_DIR/invoices

    Returns:
        (extracted_files_list, extract_root_path)
    """
    mail = connect_imap()
    try:
        msg_ids = search_invoice_messages(mail)
        all_attachments = []

        for msg_id in msg_ids:
            files = save_attachments_from_message(mail, msg_id)
            all_attachments.extend(files)

        print("\nAll downloaded attachments:")
        if not all_attachments:
            print("  (none)")
        else:
            for fp in all_attachments:
                print("  ", fp)

        extracted_files, extract_root = extract_zip_files(all_attachments)
        print("\nExtracted invoice files:")
        if not extracted_files:
            print("  (none)")
        else:
            for fp in extracted_files:
                print("  ", fp)

        return extracted_files, extract_root

    finally:
        try:
            mail.close()
        except Exception:
            pass
        mail.logout()
        print("📪 IMAP connection closed.")


def main():
    download_and_extract_invoices()


if __name__ == "__main__":
    main()
