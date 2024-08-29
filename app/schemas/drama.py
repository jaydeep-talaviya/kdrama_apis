from pydantic import BaseModel, Field
from typing import Optional,List
from bson import ObjectId


class BaseCommon(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

class GenresSchema(BaseCommon):
    genre_name: str

class TotalGenresSchema(BaseModel):
    data: List[GenresSchema]
    total_count: int

class CompaniesSchema(BaseCommon):
    tv_channel:str
    tv_channel_link:str

class TotalCompaniesSchema(BaseModel):
    data: List[CompaniesSchema]
    total_count: int

class DramaSchema(BaseModel):
    id: int
    
    class Config:
        from_attributes = True

class DramaList(BaseModel):
    dramas: List[DramaSchema]
