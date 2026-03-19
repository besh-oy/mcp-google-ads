"""
Run this script once to generate a valid refresh token.
It will open a browser for you to log in with the Google account
that has access to the Ads account.

Usage:
    pip install google-auth-oauthlib python-dotenv
    python get_refresh_token.py
"""

import os
from google_auth_oauthlib.flow import InstalledAppFlow

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CLIENT_ID = os.environ.get("GOOGLE_ADS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET must be set in .env or environment")

flow = InstalledAppFlow.from_client_config(
    {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    },
    scopes=["https://www.googleapis.com/auth/adwords"],
)

creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

print("\n✓ Success! Add this to your .env:\n")
print(f"GOOGLE_ADS_REFRESH_TOKEN={creds.refresh_token}")
