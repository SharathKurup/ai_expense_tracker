import msal
import requests
import os
import base64
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()

# ---------- Config ----------
CLIENT_ID = os.getenv("CLIENT_ID")
AUTHORITY = os.getenv("AUTHORITY")
SCOPES = os.getenv("SCOPES", "Mail.Read,User.Read").split(",")
SENDER_EMAIL = os.getenv("SENDER_EMAIL").split(",")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "attachments/locked")

GMAIL_CREDENTIALS = os.getenv("GMAIL_CREDENTIALS")
GMAIL_TOKEN = os.getenv("GMAIL_TOKEN")
GMAIL_SCOPES = os.getenv("GMAIL_SCOPES").split(",")
BANK_NAME = os.getenv("BANK_NAME").lower()
SUBJECT_QUERY = os.getenv("SUBJECT_QUERY").split(",")
CACHE_FILE = os.getenv("CACHE_FILE", "token_cache.bin")

# below env are only for privacy
bank1 = os.getenv("bank1")
bank2 = os.getenv("bank2")
bank3 = os.getenv("bank3")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------- Token Cache & Auth (Graph) ----------
cache = msal.SerializableTokenCache()
if os.path.exists(CACHE_FILE):
    cache.deserialize(open(CACHE_FILE, "r").read())

app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY,
    token_cache=cache
)

# Helper: save bytes to file (ensures safe filename)
def _save_bytes_to_file(banker, filename, content_bytes):
    filename_safe = filename.replace("/", "_").replace("\\", "_")
    #check if file name has banker name, if not add it at the beginning
    #if not filename_safe.lower().startswith(banker.lower()):
    filename_safe = f"{banker}_{filename_safe}"
    filepath = os.path.join(DOWNLOAD_DIR, filename_safe)
    if os.path.exists(filepath):
        print(f"Skipping existing file: {filename_safe}")
        return False
    with open(filepath, "wb") as f:
        f.write(content_bytes)
    print(f"Saved: {filepath}")
    return True

# --- Graph helpers ---
def _get_graph_headers():
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
    else:
        result = app.acquire_token_interactive(scopes=SCOPES)

    if "access_token" not in result:
        raise Exception(result)

    if cache.has_state_changed:
        open(CACHE_FILE, "w").write(cache.serialize())

    return {"Authorization": f"Bearer {result['access_token']}"}


def fetch_messages_graph(sender_email, subject_query, top=50):
    headers = _get_graph_headers()
    filter_parts = [f"from/emailAddress/address eq '{sender_email}'", "hasAttachments eq true"]
    if subject_query:
        # use contains for subject text
        filter_parts.append(f"contains(subject,'{subject_query}')")
    filter_str = " and ".join(filter_parts)

    messages_url = (
        f"https://graph.microsoft.com/v1.0/me/messages?$filter={filter_str}"
        f"&$select=id,subject,receivedDateTime&$top={top}"
    )

    all_messages = []
    url = messages_url
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        all_messages.extend(data.get('value', []))
        url = data.get('@odata.nextLink')

    print(f"Found {len(all_messages)} messages (Graph) with attachments from {sender_email}")
    return all_messages


def download_attachments_graph(banker, messages, headers=None):
    if headers is None:
        headers = _get_graph_headers()
    for msg in messages:
        msg_id = msg['id']
        subject = msg.get('subject', '').replace(":","")
        #convert receivedDateTime to just date
        received_date_time = msg.get('receivedDateTime', '')[:10]
        attachments_url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}/attachments"
        resp = requests.get(attachments_url, headers=headers)
        resp.raise_for_status()
        attachments = resp.json().get('value', [])

        for attach in attachments:
            if attach.get("@odata.type") == "#microsoft.graph.fileAttachment":
                #remove pdf from the filename to avoid duplicates
                filename = subject + "_" + received_date_time + ".pdf"
                #filename = attach['name'] + subject + received_date_time
                # PDF-only filter
                if not filename.lower().endswith(".pdf"):
                    continue
                content_bytes = base64.b64decode(attach['contentBytes'])
                _save_bytes_to_file(banker, filename, content_bytes)

# --- Gmail helpers ---

def get_gmail_service():

    creds = None
    if os.path.exists(GMAIL_TOKEN):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN, GMAIL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CREDENTIALS, GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GMAIL_TOKEN, "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    return service


def fetch_messages_gmail(service, sender_email, subject_query):
    q = f"from:({sender_email}) has:attachment"
    if subject_query:
        q += f" subject:({subject_query})"

    results = service.users().messages().list(userId="me", q=q).execute()
    messages = results.get("messages", [])
    print(f"Found {len(messages)} messages (Gmail) matching query: {q}")
    return messages


def _extract_parts(parts):
    # generator to walk nested parts
    for part in parts:
        yield part
        for sub in part.get('parts', []) or []:
            yield from _extract_parts([sub])


def download_attachments_gmail(banker, service, messages):
    for m in messages:
        msg_id = m['id']
        msg = service.users().messages().get(userId="me", id=msg_id).execute()
        payload = msg.get('payload', {})
        parts = payload.get('parts', [])
        # get subject and received date for filename
        headers = payload.get('headers', [])
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '').replace(":", "")
        received_date = next((h['value'] for h in headers if h['name'] == 'Date'), '')[:16].replace(" ", "_").replace(",", "")
        for part in _extract_parts(parts):
            #filename = part.get('filename')
            filename = f"{subject}_{received_date}.pdf"
            body = part.get('body', {})
            if filename and body.get('attachmentId'):
                attachment_id = body['attachmentId']
                attach = service.users().messages().attachments().get(userId="me", messageId=msg_id, id=attachment_id).execute()
                file_data = attach.get('data')
                if not file_data:
                    continue
                file_bytes = base64.urlsafe_b64decode(file_data.encode('UTF-8'))
                if not filename.lower().endswith('.pdf'):
                    continue
                _save_bytes_to_file(banker, filename, file_bytes)

# ---------- Orchestration based on MAIL_PROVIDER ----------

def main():
    print(f"Downloading for : {BANK_NAME}")
    if BANK_NAME in (bank1, "all"):
        graph_messages = fetch_messages_graph(SENDER_EMAIL[0], SUBJECT_QUERY[0])
        download_attachments_graph(bank1, graph_messages)

    if BANK_NAME in (bank2, "all"):
        service = get_gmail_service()
        gmail_messages = fetch_messages_gmail(service, SENDER_EMAIL[1], SUBJECT_QUERY[1])
        download_attachments_gmail(bank2, service, gmail_messages)

    if BANK_NAME in (bank3, "all"):
        graph_messages = fetch_messages_graph(SENDER_EMAIL[2], SUBJECT_QUERY[2])
        download_attachments_graph(bank3, graph_messages)

    print("All attachments downloaded ✅")

if __name__ == "__main__":
    main()
