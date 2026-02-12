import os
import requests
from dotenv import load_dotenv

load_dotenv()

# ---- Auth ----
tenant = os.environ["MS_TENANT_ID"]
client_id = os.environ["MS_CLIENT_ID"]
client_secret = os.environ["MS_CLIENT_SECRET"]

token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

token_resp = requests.post(
    token_url,
    data={
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    },
)
token_resp.raise_for_status()
token = token_resp.json()["access_token"]

headers = {"Authorization": f"Bearer {token}"}

# ---- STEP 1: Get SharePoint Site ----
# 🔴 CHANGE THIS to your real SharePoint URL parts
hostname = "ucsd.sharepoint.com"
site_path = "/sites/Garden31"

site_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}"

resp = requests.get(site_url, headers=headers)
print("SITE STATUS:", resp.status_code)
print("SITE BODY:", resp.text)
