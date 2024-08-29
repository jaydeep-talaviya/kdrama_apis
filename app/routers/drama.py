from fastapi import APIRouter,HTTPException, Query
from app.tasks import get_all_kdrama_once
from app.dependencies.mongo import get_mongo_db
from app.schemas import TotalGenresSchema,TotalCompaniesSchema
from typing import List,Optional

db=get_mongo_db()

drama_router = APIRouter(
    prefix="/drama",  # This is the route prefix
    tags=["drama"],   # Tags help categorize routes in the API docs
)


@drama_router.get("/genres",response_model=TotalGenresSchema)
def get_all_genres(
    limit: int = Query(10, gt=0),
     offset: int = Query(0, ge=0),
     search: Optional[str] = Query(None, min_length=1)):
    genres = list(db.genre.find().skip(offset).limit(limit))
    for item in genres:
        item["_id"] = str(item["_id"])

    if not genres:
        raise HTTPException(status_code=404, detail="No items found")
    return {'data':genres,'total_count':db.genre.count_documents({})}

@drama_router.get("/tv_channels",response_model=TotalCompaniesSchema)
def get_all_tv_channel(limit: int = Query(10, gt=0), offset: int = Query(0, ge=0)):
    tv_channels = list(db.tv_channel.find().skip(offset).limit(limit))
    for item in tv_channels:
        item["_id"] = str(item["_id"])

    if not tv_channels:
        raise HTTPException(status_code=404, detail="No Tv Channels found")
    return {'data':tv_channels,'total_count':db.tv_channel.count_documents({})}


@drama_router.post("/create_all_drama")
def create_all_drama_at_once():
    task = get_all_kdrama_once.delay()
    return {"message":"All Kdrama fetching has been started!"}

@drama_router.get("/")
def get_dramas():
    return {"message":"Get all dramas"}
