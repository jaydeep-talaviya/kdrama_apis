from fastapi import APIRouter,HTTPException, Query
from app.tasks import get_all_kdrama_once
from app.dependencies.mongo import get_mongo_db
from app.schemas import TotalGenresSchema,TotalCompaniesSchema
from typing import List,Optional
from pymongo import ASCENDING,DESCENDING

db=get_mongo_db()

drama_router = APIRouter(
    prefix="/drama",  # This is the route prefix
    tags=["drama"],   # Tags help categorize routes in the API docs
)


@drama_router.get("/genres",response_model=TotalGenresSchema)
def get_all_genres(
    limit: int = Query(10, gt=0),
    offset: int = Query(0, ge=0),
    order_by: Optional[str] = Query("genre_name"),  
    direction: Optional[str] = Query("asc")  # Default direction is ascending
    ):
    # Determine the sort direction
    sort_direction = ASCENDING if direction == "asc" else DESCENDING

    genres = list(db.genre.find().sort(order_by, sort_direction).skip(offset).limit(limit))
    for item in genres:
        item["_id"] = str(item["_id"])

    if not genres:
        raise HTTPException(status_code=404, detail="No items found")
    return {'data':genres,'total_count':db.genre.count_documents({})}

@drama_router.get("/tv_channels",response_model=TotalCompaniesSchema)
def get_all_tv_channel(
    limit: int = Query(10, gt=0), 
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, min_length=1),
    order_by: Optional[str] = Query("tv_channel"),  
    direction: Optional[str] = Query("asc")  # Default direction is ascending
    ):
    query = {}
    if search:
        query = {"tv_channel": {"$regex": search, "$options": "i"}} 
    
    sort_direction = ASCENDING if direction == "asc" else DESCENDING
    tv_channels = list(db.tv_channel.find(query).sort(order_by, sort_direction).skip(offset).limit(limit))
    for item in tv_channels:
        item["_id"] = str(item["_id"])

    if not tv_channels:
        raise HTTPException(status_code=404, detail="No Tv Channels found")
    return {'data':tv_channels,'total_count':db.tv_channel.count_documents(query)}


@drama_router.post("/create_all_drama")
def create_all_drama_at_once():
    task = get_all_kdrama_once.delay()
    return {"message":"All Kdrama fetching has been started!"}

@drama_router.get("/")
def get_dramas():
    return {"message":"Get all dramas"}
