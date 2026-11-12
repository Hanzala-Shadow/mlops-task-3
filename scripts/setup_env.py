"""Create local development credentials without overwriting existing settings."""

import os
import secrets
from pathlib import Path

path = Path(__file__).resolve().parents[1] / ".env"
if path.exists() and path.read_text().strip():
    print("Existing .env preserved.")
else:
    content = (
        "MONGO_ROOT_USERNAME=etl_admin\n"
        f"MONGO_ROOT_PASSWORD={secrets.token_hex(24)}\n"
        "MONGO_EXPRESS_USERNAME=admin\n"
        f"MONGO_EXPRESS_PASSWORD={secrets.token_hex(24)}\n"
        "MONGO_EXPRESS_PORT=8082\n"
        "MONGODB_DATABASE=currency_etl\n"
        "MONGODB_COLLECTION=exchange_rates\n"
        "ETL_BASE_CURRENCY=EUR\n"
        "ETL_TARGET_CURRENCIES=USD,GBP,JPY\n"
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.chmod(path, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(content)
    print("Created .env with generated credentials (permissions 0600).")
