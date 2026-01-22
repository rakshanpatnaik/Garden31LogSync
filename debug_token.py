import os
import requests
from dotenv import load_dotenv

load_dotenv()

tenant = os.environ["MS_TENANT_ID"]
client_id = os.environ["MS_CLIENT_ID"]
client_secret = os.environ["MS_CLIENT_SECRET"]

url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

data = {
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "client_credentials",
    "scope": "https://graph.microsoft.com/.default",
}

resp = requests.post(url, data=data)
print("Status:", resp.status_code)
print("Body:", resp.text)
