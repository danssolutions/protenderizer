import os

TED_API_URL = os.getenv("TED_API_URL", "https://ted.europa.eu/api/notices")
DB_URI = os.getenv("DB_URI", "sqlite:///data.db")
