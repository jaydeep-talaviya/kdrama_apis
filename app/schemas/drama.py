from pydantic import BaseModel, Field
from typing import Optional,List
from app.schemas.common import BaseCommon


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
