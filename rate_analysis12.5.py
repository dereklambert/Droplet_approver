import os
import pandas as pd

# These constants are only used by the optional CLI entry point.
CONTRACT_FILE_NAME = "Contracted_Rates_-_Land.xlsx"
CONTRACT_SHEET_NAME = "Contracted Rates - Land"


def find_contract_file(invoice_path: str) -> str:
    """
    Try to locate Contracted_Rates_-_Land.xlsx.
    1) Same folder as the script
    2) Same folder as the invoice file
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    invoice_dir = os.path.dirname(invoice_path)

    candidates = [
        os.path.join(script_dir, CONTRACT_FILE_NAME),
        os.path.join(invoice_dir, CONTRACT_FILE_NAME),
    ]

    for p in candidates:
        if os.path.exists(p):
            return p

    raise FileNotFoundError(
        f"Could not find {CONTRACT_FILE_NAME} in:\n"
        f"  - {script_dir}\n"
        f"  - {invoice_dir}"
    )


def build_rate_comps(invoice_path: str, contract_path: str) -> None:
    """
    Create/replace the Rate_Comps sheet inside the given invoice workbook.

    invoice_path: Excel file that came from the ServiceChannel invoice CSV.
    contract_path: Excel file downloaded from Smartsheet containing the
                   contracted landscaping rates.

    IMPORTANT:
    - Contracted rates are compared against the invoice total, not subtotal.
    - Sales tax is broken out into its own column.
    - Final Rate_Comps layout:

        Location ID
        W.O.#
        Category
        Trade
        Invoice Number
        Inv.Status
        Inv.Total
        Invoice Labor Amount
        Sales Tax
        Contracted Rate
        Rate Difference
        Approval.Status

    - Rate Difference = Inv.Total - Contracted Rate
    - Approval.Status:
        * Approved if |Rate Difference| < 0.06
        * Approved if -5 <= Rate Difference < 0 (under contract by up to $5)
        * Otherwise Review
    """
    print(f"📄 Loading invoice report: {os.path.basename(invoice_path)}")
    inv_df = pd.read_excel(invoice_path, sheet_name=0)

    print(f"📄 Loading contract rates: {os.path.basename(contract_path)}")
    land_df = pd.read_excel(contract_path, sheet_name=CONTRACT_SHEET_NAME)

    # --- Prep contract rates --------------------------------------------
    extracted = land_df["Center #"].astype(str).str.extract(r"(\d+)")[0]
    land_df["Location ID"] = pd.to_numeric(extracted, errors="coerce").astype("Int64")

    monthly = land_df["Land Maintenance Monthly w/Fall & Spring Cleanup"]
    seasonal = land_df["Land Maintenance Seasonal w/Fall & Spring Cleanup"]
    months = land_df["Billing Months"]

    land_df["Contracted Rate"] = monthly
    mask = land_df["Contracted Rate"].isna() & seasonal.notna() & months.notna()
    land_df.loc[mask, "Contracted Rate"] = seasonal[mask] / months[mask]

    rate_map = land_df.set_index("Location ID")["Contracted Rate"].to_dict()

    # --- Optional helper columns (fallbacks only) ------------------------
    # These helpers are kept in case you want to reuse them later; they are
    # NOT used for the primary mapping anymore.

    # Labor-only amount helper
    if "Invoice Labor Amount" in inv_df.columns:
        inv_df["__Inv.LaborOnly"] = pd.to_numeric(
            inv_df["Invoice Labor Amount"], errors="coerce"
        )

    # Full invoice amount helper
    if "Invoice Amount" in inv_df.columns:
        inv_df["__InvoiceAmount"] = pd.to_numeric(
            inv_df["Invoice Amount"], errors="coerce"
        )

    # Sales tax helper – from explicit tax columns if available
    tax1 = pd.to_numeric(inv_df.get("Invoice Tax Amount", 0), errors="coerce").fillna(0)
    tax2 = pd.to_numeric(inv_df.get("Invoice Tax2 Amount", 0), errors="coerce").fillna(0)
    inv_df["__SalesTax"] = (tax1 + tax2).round(2)

    # --- Pull required invoice columns ----------------------------------
    # Define which *output* fields we need and the list of candidate
    # columns in the invoice report to populate them from.
    col_candidates = {
        "Location ID": ["Location ID", "Location Number"],
        "W.O.#": ["W.O.#", "WO Tracking Number"],
        "Category": ["Category"],
        "Trade": ["Trade"],
        "Invoice Number": ["Invoice Number"],
        "Inv.Status": ["Inv.Status", "Invoice Status"],

        # Inv.Total on Rate_Comps = full invoice total (Invoice Amount)
        "Inv.Total": [
            "Invoice Amount",
            "__InvoiceAmount",
            "Inv.Total",
            "Inv.Total ",
        ],

        # Invoice Labor Amount on Rate_Comps = Invoice Labor Amount
        "Invoice Labor Amount": [
            "Invoice Labor Amount",
            "__Inv.LaborOnly",
        ],

        # Sales Tax on Rate_Comps = Invoice Tax Amount (or helper if needed)
        "Sales Tax": [
            "Invoice Tax Amount",
            "__SalesTax",
            "Sales Tax",
        ],
    }

    selected_cols: dict[str, str] = {}
    for out_name, candidates in col_candidates.items():
        for c in candidates:
            if c in inv_df.columns:
                selected_cols[out_name] = c
                break
        if out_name not in selected_cols:
            raise KeyError(
                f"Could not find any of {candidates} in invoice file columns "
                f"for output field '{out_name}'."
            )

    subset_cols_ordered = [
        selected_cols["Location ID"],
        selected_cols["W.O.#"],
        selected_cols["Category"],
        selected_cols["Trade"],
        selected_cols["Invoice Number"],
        selected_cols["Inv.Status"],
        selected_cols["Inv.Total"],
        selected_cols["Invoice Labor Amount"],
        selected_cols["Sales Tax"],
    ]

    rate_comps_df = inv_df[subset_cols_ordered].copy()

    # Rename columns exactly as desired on Rate_Comps
    rate_comps_df.columns = [
        "Location ID",
        "W.O.#",
        "Category",
        "Trade",
        "Invoice Number",
        "Inv.Status",
        "Inv.Total",             # full invoice amount (Invoice Amount)
        "Invoice Labor Amount",  # labor-only amount
        "Sales Tax",             # from Invoice Tax Amount
    ]

    # --- Contracted Rate lookup -----------------------------------------
    rate_comps_df["Contracted Rate"] = pd.to_numeric(
        rate_comps_df["Location ID"].map(rate_map), errors="coerce"
    )

    # --- Normalize numeric columns --------------------------------------
    # Clean Inv.Total (remove commas, cast to numeric)
    inv_total_numeric = pd.to_numeric(
        rate_comps_df["Inv.Total"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    rate_comps_df["Inv.Total"] = inv_total_numeric

    # Clean Invoice Labor Amount
    labor_numeric = pd.to_numeric(
        rate_comps_df["Invoice Labor Amount"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    rate_comps_df["Invoice Labor Amount"] = labor_numeric

    # Clean Sales Tax
    sales_tax_numeric = pd.to_numeric(
        rate_comps_df["Sales Tax"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    rate_comps_df["Sales Tax"] = sales_tax_numeric

    # --- Rate Difference (Inv.Total vs Contracted Rate) -----------------
    rate_comps_df["Rate Difference"] = (
        rate_comps_df["Inv.Total"] - rate_comps_df["Contracted Rate"]
    )

    # --- Approval.Status logic ------------------------------------------
    def decide_approval(diff: float) -> str:
        if pd.isna(diff):
            return "Review"

        # 1) Within $0.06 of contracted rate (up or down)
        if abs(diff) < 0.06:
            return "Approved"

        # 2) Under contract by up to $5 (negative, but not less than -5)
        if diff < 0 and diff >= -5:
            return "Approved"

        # Otherwise needs review
        return "Review"

    rate_comps_df["Approval.Status"] = rate_comps_df["Rate Difference"].apply(
        decide_approval
    )

    # --- Write back to Excel --------------------------------------------
    print("📝 Writing Rate_Comps sheet into invoice report...")
    with pd.ExcelWriter(
        invoice_path,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace",
    ) as writer:
        rate_comps_df.to_excel(writer, sheet_name="Rate_Comps", index=False)

    print("✅ Done. 'Rate_Comps' sheet created/updated.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print(
            "Usage:\n"
            "  python rate_analysis.py <invoice_excel_path> <contract_excel_path>\n\n"
            "Example:\n"
            "  python rate_analysis.py invoice_report_-_financial_details.xlsx "
            "Contracted_Rates_-_Land.xlsx"
        )
        raise SystemExit(1)

    invoice_file = sys.argv[1]
    contract_file = sys.argv[2]
    build_rate_comps(invoice_file, contract_file)
