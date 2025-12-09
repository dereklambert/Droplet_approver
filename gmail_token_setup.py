from __future__ import annotations
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def main():
    """Run once on a machine with a browser to create token.json."""
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing existing Gmail token...")
            creds.refresh(Request())
        else:
            print("Running Gmail OAuth browser flow (local server)...")
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            # This starts a tiny local web server and opens your browser.
            # Just follow the prompts in THAT browser window.
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token_file:
            token_file.write(creds.to_json())
            print("✅ token.json written.")


if __name__ == "__main__":
    main()

