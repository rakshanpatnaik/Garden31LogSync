# subscribe.py
import os
import requests
from dotenv import load_dotenv

from main import get_graph_token  # reuse helper

load_dotenv()

GRAPH_SUBSCRIPTIONS_URL = "https://graph.microsoft.com/v1.0/subscriptions"


def create_subscription():
    """
    Creates a subscription for changes in the configured drive/folder.
    This uses the "driveItem" changes for a specific drive.

    You must set in .env:
      MS_NOTIFICATION_URL   → your public https://.../graph/webhook
      MS_SUBSCRIPTION_RESOURCE → e.g. /drives/{drive-id}/root
                                 or /drives/{drive-id}/root:/Tend Exports
    """
    token = get_graph_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    notification_url = os.environ["MS_NOTIFICATION_URL"]
    resource = os.environ["MS_SUBSCRIPTION_RESOURCE"]
    client_state = os.environ.get("MS_CLIENT_STATE", "garden31-secret")

    # Expiration must be within Graph's limits (often <= 3 days for OneDrive/SharePoint)
    # Example: now + 48 hours (adjust as needed).
    from datetime import datetime, timedelta, timezone
    expiration = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()

    body = {
        "changeType": "created,updated",
        "notificationUrl": notification_url,
        "resource": resource,
        "expirationDateTime": expiration,
        "clientState": client_state,
    }

    resp = requests.post(GRAPH_SUBSCRIPTIONS_URL, headers=headers, json=body)
    print("Status:", resp.status_code)
    print("Response:", resp.text)


if __name__ == "__main__":
    create_subscription()
