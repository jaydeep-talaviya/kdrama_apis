from pydantic import BaseModel, Field
from typing import Optional,List,Union
from app.schemas.common import BaseCommon


class MovieExtraInfoSchema(BaseModel):
    genres:Optional[List[str]]=[]
    directed_bys:Optional[List[dict]]=None
    written_bys:Optional[List[dict]]=None
    casts_info:Optional[List[dict]]=None


class MovieSchema(BaseCommon):
    movie_name: str
    image_url: str
    other_names:List[str]
    airing_date:Optional[Union[str,bool]]=None
    duration:Optional[Union[str,bool]] = None
    last_paragraph:Optional[str] = None
    extra_info: Optional[MovieExtraInfoSchema]=None

class TotalMovieSchema(BaseModel):
    data: List[MovieSchema]
    total_count: int