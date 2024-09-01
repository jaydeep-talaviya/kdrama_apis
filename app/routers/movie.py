from fastapi import APIRouter,Query,HTTPException
from app.tasks import get_all_movie_once
from typing import Optional
from pymongo import ASCENDING,DESCENDING
from app.dependencies.mongo import get_mongo_db
from bson import ObjectId
from app.schemas.movie import TotalMovieSchema

db=get_mongo_db()

movie_router = APIRouter(
    prefix="/movie",  # This is the route prefix
    tags=["movie"],   # Tags help categorize routes in the API docs
)

@movie_router.post("/create_all_movie")
def create_all_movie_at_once():
    task = get_all_movie_once.delay()
    return {"message":"All K-Movie fetching has been started!"}

@movie_router.get("/",response_model=TotalMovieSchema)
def get_movies(limit: int = Query(10, gt=0), 
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, min_length=1),
    order_by: Optional[str] = Query("movie_name"),  
    direction: Optional[str] = Query("asc")
):
    query = {}
   
    if search:
         query = {
            "$or": [
                {"movie_name": {"$regex": search, "$options": "i"}},
                {"other_names": {"$regex": search, "$options": "i"}}
            ]
        }
    sort_direction = ASCENDING if direction == "asc" else DESCENDING
    movies = list(db.movie.find(query).sort(order_by, sort_direction).limit(limit).skip(offset))
    
    for movie in movies:
        # extra info
        extra_info = db.movie_extra_info.find_one({"movie_id":movie['_id']},{"_id":0,"movie_id":0,"images":0})
        if extra_info.get('genres'):
            genres = list(map(lambda x:x['genre_name'],db.genre.find({'_id':{"$in":extra_info['genres']}},{"_id":0})))
            extra_info['genres'] = genres
        if extra_info.get('directed_bys'):
            directed_bys = list(map(lambda x:{**x,"_id":str(x['_id'])},db.person.find({'_id':{"$in":extra_info['directed_bys']}},{"_id":1,"name":1})))
            extra_info['directed_bys'] = directed_bys
        if extra_info.get('written_bys'):
            written_bys = list(map(lambda x:{**x,"_id":str(x['_id'])},db.person.find({'_id':{"$in":extra_info['written_bys']}},{"_id":1,"name":1})))
            extra_info['written_bys'] = written_bys
        if extra_info.get('casts_ids') or extra_info.get('other_cast_info'):
            cast_of_drama = list(map(lambda x:x['cast_id'],db.cast_of_drama.find({'_id':{'$in':extra_info.get('casts_ids',[])+extra_info.get('other_cast_info',[])}},{'_id':0,"cast_id":1}).limit(5)))
            casts = list(map(lambda x:{**x,"_id":str(x['_id'])},db.person.find({'_id':{"$in":cast_of_drama}},{"_id":1,"name":1})))
            extra_info['casts_info'] = casts
            extra_info.pop("casts_ids",None)
            extra_info.pop("other_cast_info",None)
        movie['extra_info'] = extra_info
        print(">movie",movie)

    for item in movies:
        item["_id"] = str(item["_id"])
    if not movies:
        raise HTTPException(status_code=404, detail="No Movie found")
    return {"data": movies,"total_count":db.movie.count_documents(query)}