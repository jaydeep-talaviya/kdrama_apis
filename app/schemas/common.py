from pydantic import BaseModel, Field
from typing import Optional,List
from bson import ObjectId

class BaseCommon(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}