from dotenv import load_dotenv
import os

load_dotenv()

CONFIG = {
    "redash_domain": os.getenv("REDASH_DOMAIN"),
    "webhook_err": os.getenv("WEBHOOK_URL"),
    "service_account_path": os.getenv("SERVICE_ACCOUNT_PATH"),
    "sheet_id": os.getenv("SHEET_ID"),
    "data_path": os.getenv("DATA_PATH"),
    "log_path": os.getenv("LOG_PATH"),
    "pbi_title": os.getenv("PBI_TITLE"),
    "scopes": [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
}
