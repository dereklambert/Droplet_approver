import os
import sys
import pandas as pd

from DL_contract_rate_land import download_contract_rates
from rate_analysis import build_rate_comps
from fetch_invoices import download_and_extract_invoices
from approveINV_email import run_approvals


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ATTACHMENT_DIR = os.path.join(BASE_DIR, "invoice_attachments")
PREFERRED_NAME = "invoice_report_-_financial_details.csv"


def ensure_folder(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def find_invoice_csv(extract_dir: str) -> str:
    """
    Choose the correct ServiceChannel invoice CSV from the extracted folder.

    Preference order:
      1) invoice_report_-_financial_details.csv
      2) any CSV whose name contains "financial_details"
      3) any CSV whose name contains "accounting_details"
      4) first CSV file we see

    This prevents us from accidentally using the accounting_details report,
    which does not contain the Location ID / Subtotal columns that the
    rate_analysis logic depends on.
    """
    candidates = []

    for root, _, files in os.walk(extract_dir):
        for f in files:
            if f.lower().endswith(".csv"):
                full = os.path.join(root, f)
                candidates.append(full)

    if not candidates:
        raise FileNotFoundError(
            f"No CSV invoice file found in extracted directory: {extract_dir}"
        )

    print("\nAvailable CSV files in extracted folder:")
    for c in candidates:
        print("  -", os.path.basename(c))

    # 1) Exact preferred name
    preferred_exact = os.path.join(extract_dir, PREFERRED_NAME)
    for c in candidates:
        if os.path.normpath(c) == os.path.normpath(preferred_exact):
            print(f"\n📌 Using preferred exact CSV: {c}")
            return c

    # 2) Name contains "financial_details"
    for c in candidates:
        if "financial_details" in os.path.basename(c).lower():
            print(f"\n📌 Using CSV containing 'financial_details': {c}")
            return c

    # 3) Fallback: "accounting_details"
    for c in candidates:
        if "accounting_details" in os.path.basename(c).lower():
            print(f"\n📌 WARNING: Using 'accounting_details' CSV as fallback: {c}")
            print("    Downstream logic may not work if required columns are missing.")
            return c

    # 4) Final fallback: first CSV
    print("\n📌 WARNING: Using first CSV file as last-resort fallback:", candidates[0])
    return candidates[0]


def convert_csv_to_xlsx(csv_path: str) -> str:
    """
    Convert the SC invoice CSV to an Excel file with sheet 'Invoice_Report'.
    Returns the path to the new XLSX.
    """
    print(f"\n📄 Converting CSV to Excel: {csv_path}")
    df = pd.read_csv(csv_path)

    invoice_dir = os.path.dirname(csv_path)
    xlsx_path = os.path.join(
        invoice_dir,
        os.path.splitext(os.path.basename(csv_path))[0] + ".xlsx",
    )

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Invoice_Report", index=False)

    print(f"✅ Invoice Excel created at: {xlsx_path}")
    return xlsx_path


def main():
    print("==== DROPLET PIPELINE START ====")

    # 1) Ensure attachment folder exists
    ensure_folder(ATTACHMENT_DIR)

    # 2) Download contract rates from Smartsheet
    print("\n==== STEP 1: Download contract rates from Smartsheet ====")
    contract_path = download_contract_rates()

    # 3) Fetch latest invoice ZIP from Gmail and extract
    print("\n==== STEP 2: Download invoice ZIP from Gmail and extract ====")
    extracted_files, extract_root = download_and_extract_invoices()

    if not extracted_files:
        print("❌ No files extracted from invoice ZIP. Exiting.")
        sys.exit(1)

    # 4) Locate the SC invoice CSV
    print("\n==== STEP 3: Locate invoice CSV ====")
    invoice_csv = find_invoice_csv(extract_root)
    print(f"Using invoice CSV: {invoice_csv}")

    # 5) Convert CSV to Excel
    print("\n==== STEP 4: Convert CSV to Excel ====")
    invoice_xlsx = convert_csv_to_xlsx(invoice_csv)

    # 6) Build Rate_Comps inside the invoice workbook
    print("\n==== STEP 5: Build Rate_Comps sheet ====")
    build_rate_comps(invoice_xlsx, contract_path)

    # 7) Approve invoices using the updated workbook
    print("\n==== STEP 6: Approve invoices & send email ====")
    run_approvals(invoice_xlsx)

    print("\n🎉 Droplet full-auto pipeline complete.")


if __name__ == "__main__":
    main()
