from app.db.session import get_postgres_db
from fastapi import Depends

def get_db_dependency(db=Depends(get_postgres_db)):
    return db
