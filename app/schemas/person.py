from typing import Optional,List
from app.schemas.common import BaseCommon
from pydantic import BaseModel, Field

class PersonSchema(BaseCommon):
    name: str
    gender: str
    jobs:Optional[List[str]]=[]
    other_names:str
    person_image:Optional[str]=None
    birth_of_date:Optional[str]=None

class TotalPersonSchema(BaseModel):
    data: List[PersonSchema]
    total_count: int
