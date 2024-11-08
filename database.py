import os
from typing import Annotated
from fastapi import Depends
from sqlalchemy import URL
from sqlmodel import Session, create_engine

connection_string = URL.create(
    'postgresql',
    username='koyeb-adm',
    password=os.getenv('DB_PASSWORD'),
    host='ep-icy-credit-a23gjiq9.eu-central-1.pg.koyeb.app',
    database='koyebdb',
)

connect_args = {"check_same_thread": False}

engine = create_engine(connection_string, connect_args)

def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]