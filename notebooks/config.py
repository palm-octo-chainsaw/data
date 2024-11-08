import os
import pandas as pd  # noqa: F401
from sqlalchemy import URL, create_engine  # noqa: F401
from dotenv import load_dotenv


load_dotenv()

DB_CONNECTION_STRING = URL.create(
    drivername='postgresql',
    username='koyeb-adm',
    password=os.getenv('DB_PASSWORD'),
    host='ep-icy-credit-a23gjiq9.eu-central-1.pg.koyeb.app',
    database='weather',
)
