from pydantic import BaseModel, Field
from typing import Optional,List,Union
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


class DramaExtraInfoSchema(BaseModel):
    genres:Optional[List[str]]=[]
    directed_bys:Optional[List[dict]]=None
    written_bys:Optional[List[dict]]=None
    casts_info:Optional[List[dict]]=None


class DramaSchema(BaseCommon):
    drama_name: str
    image_url: str
    other_names:List[str]
    tv_channel:Optional[str]=None
    airing_dates_start:Optional[Union[str,bool]]=None
    airing_dates_end:Optional[Union[str,bool]]=None
    last_paragraph:Optional[str] = None
    extra_info: Optional[DramaExtraInfoSchema]=None

class TotalDramaSchema(BaseModel):
    data: List[DramaSchema]
    total_count: int
