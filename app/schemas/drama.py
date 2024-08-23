from pydantic import BaseModel


class DramaSchema(BaseModel):
    id: int
    
    class Config:
        orm_mode = True

class DramaList(BaseModel):
    dramas: List(DramaSchema)
