import msal
import requests
import os
import base64
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ---------- Config ----------
CLIENT_ID = os.getenv("CLIENT_ID")
AUTHORITY = os.getenv("AUTHORITY")
SCOPES = os.getenv("SCOPES", "Mail.Read,User.Read").split(",")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "attachments/locked")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------- Token Cache & Auth ----------
CACHE_FILE = os.getenv("CACHE_FILE", "token_cache.bin")
cache = msal.SerializableTokenCache()
if os.path.exists(CACHE_FILE):
    cache.deserialize(open(CACHE_FILE, "r").read())

app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY,
    token_cache=cache
)

accounts = app.get_accounts()
if accounts:
    result = app.acquire_token_silent(SCOPES, account=accounts[0])
else:
    result = app.acquire_token_interactive(scopes=SCOPES)

if "access_token" not in result:
    raise Exception(result)

if cache.has_state_changed:
    open(CACHE_FILE, "w").write(cache.serialize())

headers = {"Authorization": f"Bearer {result['access_token']}"}

# ---------- Step 1: Fetch all messages from sender with attachments ----------
# Add subject filter: only messages with subject "AXIS BANK : Statement"
messages_url = (
    f"https://graph.microsoft.com/v1.0/me/messages?$filter=from/emailAddress/address eq '{SENDER_EMAIL}' "
    f"and hasAttachments eq true "
    f"and contains(subject,'Statement') "
    f"&$select=id,subject,receivedDateTime&$top=50"
)

all_messages = []
url = messages_url

while url:
    resp = requests.get(url, headers=headers).json()
    all_messages.extend(resp.get('value', []))
    url = resp.get('@odata.nextLink')  # pagination link if more messages

print(f"Found {len(all_messages)} messages with attachments from {SENDER_EMAIL}")

# ---------- Step 2: Download PDF attachments ----------
for msg in all_messages:
    msg_id = msg['id']
    attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}/attachments"
    attachments_resp = requests.get(attachments_url, headers=headers).json()
    attachments = attachments_resp.get('value', [])

    for attach in attachments:
        if attach.get("@odata.type") == "#microsoft.graph.fileAttachment":
            filename = attach['name']
            # PDF-only filter
            if not filename.lower().endswith(".pdf"):
                continue
            # Safe filename
            filename = filename.replace("/", "_").replace("\\", "_")
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.exists(filepath):
                print(f"Skipping existing file: {filename}")
                continue
            content_bytes = base64.b64decode(attach['contentBytes'])
            with open(filepath, "wb") as f:
                f.write(content_bytes)
            print(f"Saved: {filepath}")

print("All attachments downloaded ✅")
