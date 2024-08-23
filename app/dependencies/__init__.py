from app.db.session import get_db
from fastapi import Depends

def get_db_dependency(db=Depends(get_db)):
    return db
