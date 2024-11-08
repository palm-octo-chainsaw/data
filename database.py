from typing import Annotated
from fastapi import Depends
from sqlmodel import Session, create_engine
from config import DB_CONNECTION_STRING


CONNECT_ARGS = {'check_same_thread': False}

ENGINE = create_engine(DB_CONNECTION_STRING, CONNECT_ARGS)


def get_session():
    with Session(ENGINE) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]
