import os
import pandas as pd
from sqlalchemy import URL, create_engine
from dotenv import load_dotenv


load_dotenv()

connection_string = URL.create(
    drivername='postgresql',
    username='koyeb-adm',
    password=os.getenv('DB_PASSWORD'),
    host='ep-icy-credit-a23gjiq9.eu-central-1.pg.koyeb.app',
    database='weather',
)