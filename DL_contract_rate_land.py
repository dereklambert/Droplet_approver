import credentials as cred
import requests
import re


# Smartsheet Sheet ID
SHEET_ID = "6819980372823940"


def download_contract_rates(sheet_id: str = SHEET_ID) -> str:
    """
    Download the Smartsheet as an Excel file and return the local path.

    The file name is based on the actual Smartsheet name, sanitized so it is
    safe to use as a Windows filename (invalid characters removed, spaces -> _).
    """
    # 1) Get sheet metadata to determine actual sheet name
    headers_json = {
        "Authorization": f"Bearer {cred.smtoken}",
        "Accept": "application/json",
    }
    meta_response = requests.get(
        f"https://api.smartsheet.com/2.0/sheets/{sheet_id}",
        headers=headers_json,
        timeout=60,
    )

    if meta_response.status_code != 200:
        raise RuntimeError(
            f"Error retrieving sheet metadata ({meta_response.status_code}): "
            f"{meta_response.text}"
        )

    sheet_name = meta_response.json().get("name", "Smartsheet_Download")

    # Sanitize filename (remove invalid characters)
    sanitized_name = re.sub(r'[\\/*?:"<>|]', "", sheet_name).replace(" ", "_")
    output_file = f"{sanitized_name}.xlsx"

    print(f"📄 Sheet name detected: {sheet_name}")
    print(f"💾 Saving contracted rates as: {output_file}")

    # 2) Download sheet as Excel
    headers_excel = {
        "Authorization": f"Bearer {cred.smtoken}",
        "Accept": "application/vnd.ms-excel",
    }

    excel_response = requests.get(
        f"https://api.smartsheet.com/2.0/sheets/{sheet_id}",
        headers=headers_excel,
        timeout=120,
    )

    if excel_response.status_code != 200:
        raise RuntimeError(
            f"Error downloading Excel ({excel_response.status_code}): "
            f"{excel_response.text}"
        )

    with open(output_file, "wb") as f:
        f.write(excel_response.content)

    print(f"✅ File downloaded successfully: {output_file}")
    return output_file


if __name__ == "__main__":
    download_contract_rates()
