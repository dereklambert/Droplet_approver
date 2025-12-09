import sys
import requests
import pandas as pd
import credentials as Cred
import traceback
import os
import base64
import mimetypes

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- CONFIG ----------
API_HOST = "api.servicechannel.com"  # or "sb2api.servicechannel.com"
BASE_API_URL_V3 = f"https://{API_HOST}/v3"

SUBSCRIBER_ID = 2014917421  # int

APPROVAL_CODE_DEFAULT = "5440-102100"
APPROVAL_COMMENTS_DEFAULT = "Approved by Derek- All invoices compared to contracted rates."
CATEGORY_FALLBACK = "MAINTENANCE"   # <- default if Category cell is empty

# ---------- EMAIL CONFIG ----------
EMAIL_TO = "Dlambert@kc-education.com"
EMAIL_SUBJECT = "Landscaping Invoice Approvals Status"

# ---------- AUTH CONFIG ----------
TOKEN_URL = "https://login.servicechannel.com/oauth/token"

authcode = Cred.authcode      # "Basic XXXX"
username = Cred.user_name
password = Cred.password

# ---------- GMAIL API CONFIG ----------
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def get_access_token() -> str:
    """Uses Resource Owner Password grant to get a ServiceChannel access_token."""
    headers = {
        "Authorization": authcode,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "password",
        "username": username,
        "password": password,
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    if not resp.ok:
        raise RuntimeError(
            f"Failed to obtain access token. Status {resp.status_code}, body: {resp.text}"
        )
    token_json = resp.json()
    access_token = token_json.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in response: {token_json}")
    return access_token


def get_or_refresh_token(token_box: dict) -> str:
    """
    Return the current token from token_box, fetching a new one if needed.
    token_box is a dict like {"access_token": "..."} that we can mutate.
    """
    token = token_box.get("access_token")
    if not token:
        token_box["access_token"] = get_access_token()
    return token_box["access_token"]


# ---------- GMAIL API HELPER ----------
def get_gmail_service():
    """
    Build and return a Gmail API service using token.json created by
    gmail_token_setup_droplet.py.
    """
    token_path = "token.json"
    if not os.path.exists(token_path):
        raise RuntimeError(
            "Gmail token.json not found. Run gmail_token_setup_droplet.py first "
            "to complete the OAuth flow and create token.json on the droplet."
        )

    creds = Credentials.from_authorized_user_file(token_path, GMAIL_SCOPES)
    return build("gmail", "v1", credentials=creds)


# ---------- LOOKUP INVOICE META BY WO (OData v3) ----------
def lookup_invoice_by_wo(wo_number: str, token_box: dict, cache: dict) -> dict | None:
    """
    Look up invoice metadata using WoTrackingNumber via OData.

    Returns dict:
      {"Id": int, "Trade": str, "ApprovalCode": str, "Number": str}
    or None if not found / error.
    """
    wo_key = (wo_number or "").strip()
    if not wo_key:
        return None

    if wo_key in cache:
        return cache[wo_key]

    url = f"{BASE_API_URL_V3}/odata/invoices"

    if wo_key.isdigit():
        filter_expr = f"WoTrackingNumber eq {wo_key}"
    else:
        filter_expr = f"WoTrackingNumber eq '{wo_key}'"

    params = {
        "$filter": filter_expr,
        "$top": 1,
    }

    def do_request() -> requests.Response:
        access_token = get_or_refresh_token(token_box)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "sc-subscription-id": str(SUBSCRIBER_ID),
        }
        return requests.get(url, headers=headers, params=params, timeout=15)

    try:
        resp = do_request()
    except requests.RequestException as e:
        print(f"⚠️ Network error looking up WO {wo_key}: {e}")
        cache[wo_key] = None
        return None

    if resp.status_code == 401:
        print(f"🔁 401 Unauthorized for WO {wo_key} — refreshing token and retrying once…")
        try:
            token_box["access_token"] = get_access_token()
            resp = do_request()
        except requests.RequestException as e:
            print(f"⚠️ Network error after token refresh for WO {wo_key}: {e}")
            cache[wo_key] = None
            return None

        if resp.status_code == 401:
            print(f"❌ Still unauthorized for WO {wo_key} after token refresh: {resp.text}")
            cache[wo_key] = None
            return None

    if not resp.ok:
        print(f"⚠️ Failed lookup for WO {wo_key}: {resp.status_code} {resp.text}")
        cache[wo_key] = None
        return None

    data = resp.json().get("value", [])
    if not data:
        print(f"⚠️ No invoice found in OData for WO {wo_key}")
        cache[wo_key] = None
        return None

    item = data[0]
    meta = {
        "Id": item.get("Id"),
        "Trade": item.get("Trade"),
        "ApprovalCode": item.get("ApprovalCode"),
        "Number": item.get("Number"),
    }

    if meta["Id"] is None:
        print(f"⚠️ Missing Id for WO {wo_key}")
        cache[wo_key] = None
        return None

    cache[wo_key] = meta
    return meta


# ---------- BUILD DATAFRAME FROM Rate_Comps ----------
def load_rate_comps(invoice_path: str) -> pd.DataFrame:
    """
    Loads the Rate_Comps sheet.

    Expected columns in Rate_Comps (per new layout):
    - Location ID
    - W.O.#
    - Category
    - Trade
    - Invoice Number
    - Inv.Status
    - Inv.Total
    - Invoice Labor Amount
    - Sales Tax
    - Contracted Rate
    - Rate Difference
    - Approval.Status
    """
    print(f"📄 Loading Rate_Comps from: {invoice_path}")
    rc_df = pd.read_excel(invoice_path, sheet_name="Rate_Comps")

    expected_cols = [
        "Location ID",
        "W.O.#",
        "Category",
        "Trade",
        "Invoice Number",
        "Inv.Status",
        "Inv.Total",
        "Invoice Labor Amount",
        "Sales Tax",
        "Contracted Rate",
        "Rate Difference",
        "Approval.Status",
    ]

    missing = [c for c in expected_cols if c not in rc_df.columns]
    if missing:
        raise KeyError(f"Rate_Comps sheet is missing columns: {missing}")

    df = rc_df[expected_cols].copy()
    return df


# ---------- APPROVE VIA API ----------
def approve_invoice(session: requests.Session, token_box: dict, invoice_id: int,
                    category: str, approval_code: str | None) -> bool:
    """
    Approves a single invoice.

    Returns True if:
      - invoice is newly approved, OR
      - ServiceChannel says it already has this status (already approved).

    Returns False for all other errors.
    """
    url = f"{BASE_API_URL_V3}/invoices/{invoice_id}/approve"

    code_to_use = (approval_code or "").strip() or APPROVAL_CODE_DEFAULT
    category_to_use = (category or "").strip().upper() or CATEGORY_FALLBACK

    params = {
        "approvalCode": code_to_use,
        "comments": APPROVAL_COMMENTS_DEFAULT,
        "category": category_to_use,
    }

    def do_request() -> requests.Response:
        access_token = get_or_refresh_token(token_box)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "sc-subscription-id": str(SUBSCRIBER_ID),
        }
        return session.put(url, headers=headers, params=params, timeout=15)

    try:
        resp = do_request()
    except requests.RequestException as e:
        print(f"❌ Network error approving Invoice {invoice_id} "
              f"(category={category_to_use}): {e}")
        return False

    if resp.status_code == 401:
        print(f"🔁 401 Unauthorized approving Invoice {invoice_id} — refreshing token and retrying once…")
        try:
            token_box["access_token"] = get_access_token()
            resp = do_request()
        except requests.RequestException as e:
            print(f"❌ Network error after token refresh for Invoice {invoice_id}: {e}")
            return False

        if resp.status_code == 401:
            print(f"❌ Still unauthorized approving Invoice {invoice_id}: {resp.text}")
            return False

    # ✅ Success: newly approved
    if resp.status_code in (200, 204):
        return True

    # ✅ Treat "already had this status" as success (already approved)
    if resp.status_code == 403:
        msg = resp.text or ""
        if "already had this status" in msg:
            print("   ℹ️ Invoice already approved in ServiceChannel — skipping cleanly.")
            return True

    # ❌ Any other 4xx/5xx is a real failure
    print(
        f"❌ Failed to approve Invoice {invoice_id} "
        f"(category={category_to_use}): {resp.status_code} {resp.text}"
    )
    return False


# ---------- EMAIL SENDER (GMAIL API) ----------
def send_status_email(approved_count: int, failed_count: int, review_count: int,
                      error_text: str | None = None,
                      attachment_path: str | None = None) -> None:
    """
    Send a summary email via the Gmail API using token.json.

    Requirements on the droplet:
        - credentials.json and token.json in the working directory
        - Environment variable GMAIL_ADDRESS set to the authorized Gmail address
        - attachment_path (optional): path to an .xlsx workbook to attach
    """
    from_addr = os.environ.get("GMAIL_ADDRESS")

    if not from_addr:
        print("⚠️ Skipping status email: GMAIL_ADDRESS not set in env.")
        return

    try:
        body_html = f"""
        <html>
        <body>
        <p>Hi Derek,</p>
        <p>
          The landscaping invoices have been validated to be within the contracted
          rates (per the Rate_Comps analysis). Approvals have been sent for rows
          marked as <b>Approved</b>.
        </p>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;">
          <tr>
            <th align="left">Status</th>
            <th align="right">Count</th>
          </tr>
          <tr>
            <td>Approved</td>
            <td align="right">{approved_count}</td>
          </tr>
          <tr>
            <td>Not Approved (errors)</td>
            <td align="right">{failed_count}</td>
          </tr>
          <tr>
            <td>Need Review (skipped)</td>
            <td align="right">{review_count}</td>
          </tr>
        </table>
        """

        if error_text:
            safe_error = error_text.replace("<", "&lt;").replace(">", "&gt;")
            body_html += f"""
            <p><b>Unhandled error encountered:</b></p>
            <pre style="font-family: Consolas, monospace; font-size: 11px;
                        border:1px solid #ccc; padding:8px; background:#f9f9f9;">
{safe_error}
            </pre>
            """

        body_html += """
        <p>Regards,<br/>Invoice Auto-Approver Bot</p>
        </body>
        </html>
        """

        # Build a multipart message (HTML body + optional attachment)
        msg = MIMEMultipart()
        msg["Subject"] = EMAIL_SUBJECT
        msg["From"] = from_addr
        msg["To"] = EMAIL_TO

        # Attach the HTML body
        msg.attach(MIMEText(body_html, "html"))

        # Optionally attach the Excel workbook
        if attachment_path:
            if os.path.exists(attachment_path):
                ctype, encoding = mimetypes.guess_type(attachment_path)
                if ctype is None or encoding is not None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)

                with open(attachment_path, "rb") as f:
                    part = MIMEBase(maintype, subtype)
                    part.set_payload(f.read())

                encoders.encode_base64(part)
                filename = os.path.basename(attachment_path)
                part.add_header("Content-Disposition", "attachment", filename=filename)
                msg.attach(part)

                print(f"📎 Attached workbook: {attachment_path}")
            else:
                print(f"⚠️ Attachment path does not exist, skipping: {attachment_path}")

        # Encode and send via Gmail API
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        message_body = {"raw": raw}

        service = get_gmail_service()
        sent = service.users().messages().send(userId="me", body=message_body).execute()

        msg_id = sent.get("id")
        print(f"📧 Status email sent to {EMAIL_TO} (Gmail message id: {msg_id})")

    except HttpError as e:
        print(f"⚠️ Gmail API error sending status email: {e}")
    except Exception as e:
        print(f"⚠️ Failed to send status email: {e}")


# ---------- MAIN WORKER ----------
def run_approvals(invoice_file: str) -> None:
    """
    Core driver function.

    invoice_file: path to the Excel workbook that already contains a
                  Rate_Comps sheet with Approval.Status set to Approved/Review.
    """
    approved_count = 0
    failed_count = 0
    review_count = 0
    error_text = None

    token_box: dict[str, str] = {}

    try:
        print("🔑 Getting access token…")
        token_box["access_token"] = get_access_token()

        df = load_rate_comps(invoice_file)

        # Determine which rows are Approved vs need Review,
        # based purely on Approval.Status.
        approved_mask = (
            df["Approval.Status"]
            .astype(str)
            .str.strip()
            .str.upper()
            == "APPROVED"
        )

        to_approve = df[approved_mask].copy()
        to_skip = df[~approved_mask].copy()
        review_count = len(to_skip)

        if not to_skip.empty:
            print("ℹ️ These invoices will be skipped (Approval.Status != 'Approved'):")
            print(
                to_skip[
                    [
                        "Invoice Number",
                        "W.O.#",
                        "Location ID",
                        "Category",
                        "Trade",
                        "Inv.Total",
                        "Contracted Rate",
                        "Rate Difference",
                        "Approval.Status",
                    ]
                ]
            )
            print()

        if to_approve.empty:
            print("⚠️ No invoices have Approval.Status = 'Approved'. Nothing to approve.")
            return

        print(f"✅ {len(to_approve)} invoices will be processed line-by-line…")

        session = requests.Session()
        lookup_cache: dict[str, dict | None] = {}
        successes = 0
        failures = 0

        for idx, (_, row) in enumerate(to_approve.iterrows(), start=1):
            loc = row["Location ID"]
            inv_num = row["Invoice Number"]
            wo_num = row["W.O.#"]
            category = row.get("Category")
            trade_from_sheet = (row.get("Trade") or "").strip()
            approval_status = row.get("Approval.Status")
            rate_diff = row.get("Rate Difference")

            print(
                f"\n[{idx}/{len(to_approve)}] Processing row: "
                f"Invoice {inv_num}, WO {wo_num}, Location {loc}, "
                f"Trade={trade_from_sheet}, Approval.Status={approval_status}, "
                f"RateDiff={rate_diff}"
            )

            meta = lookup_invoice_by_wo(str(wo_num), token_box, lookup_cache)
            if not meta:
                print(f"   ⚠️ Skipping: Could not find invoice metadata for WO {wo_num}.")
                failures += 1
                continue

            invoice_id = meta["Id"]
            trade_from_api = (meta.get("Trade") or "").strip()
            approval_code = meta.get("ApprovalCode")
            inv_num_api = meta.get("Number") or inv_num

            print(
                f"   🔎 Found InvoiceId={invoice_id}, API InvoiceNumber={inv_num_api}, "
                f"API Trade={trade_from_api}"
            )

            ok = approve_invoice(session, token_box, invoice_id, category, approval_code)
            if ok:
                print("   ✅ Approved")
                successes += 1
            else:
                print("   ❌ Failed to approve this invoice.")
                failures += 1

        approved_count = successes
        failed_count = failures

        print("\n--------------------------")
        print(f"🏁 Completed — Approved: {approved_count}, Failed: {failed_count}, Need review: {review_count}")
        print("--------------------------")

    except Exception:
        error_text = traceback.format_exc()
        print("❌ Unhandled error in script:")
        print(error_text)

    finally:
        # Attach the same workbook that was processed
        send_status_email(
            approved_count,
            failed_count,
            review_count,
            error_text,
            attachment_path=invoice_file,
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage:\n"
            '  python approveINV_email.py <invoice_excel_path>\n\n'
            "Example:\n"
            '  python approveINV_email.py dashboard-invoice_reports/invoice_report_-_financial_details.xlsx'
        )
        raise SystemExit(1)

    run_approvals(sys.argv[1])
