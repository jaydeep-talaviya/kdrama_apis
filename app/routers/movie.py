from fastapi import APIRouter,Query,HTTPException
from app.tasks import get_all_movie_once
from typing import Optional,List
from pymongo import ASCENDING,DESCENDING
from app.dependencies.mongo import get_mongo_db
from bson import ObjectId
from app.schemas.movie import TotalMovieSchema
from app.utilities.common_functions import get_person_first_image
from datetime import datetime

db=get_mongo_db()

movie_router = APIRouter(
    prefix="/movie",  # This is the route prefix
    tags=["movie"],   # Tags help categorize routes in the API docs
)

@movie_router.post("/create_all_movie")
def create_all_movie_at_once():
    task = get_all_movie_once.delay()
    return {"message":"All K-Movie fetching has been started!"}

@movie_router.get("/jobs")
def get_all_jobs():
    total_jobs = db.person.aggregate([
    { "$unwind": "$jobs" },   # Deconstructs the jobs array
    { "$group": { "_id": None, "jobs": { "$addToSet": "$jobs" } } },  # Groups and accumulates unique jobs
    { "$project": { "_id": 0, "jobs": 1 } }  # Projects only the jobs field
    ])
    return {"data":list(total_jobs),'total_count':len(list(total_jobs))}


@movie_router.get("/",response_model=TotalMovieSchema)
def get_movies(limit: int = Query(10, gt=0), 
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, min_length=1),
    order_by: Optional[str] = Query("movie_name"),  
    direction: Optional[str] = Query("asc"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    genres: Optional[List[str]] = Query(None),

):
    query = {}
   
    if search:
        query = {
            "$or": [
                {"movie_name": {"$regex": search, "$options": "i"}},
                {"other_names": {"$regex": search, "$options": "i"}}
            ]
        }
    


    # Exclude records with null or missing airing_date and validate date format
    query["airing_date"] = {
        "$exists": True,  # Ensure airing_date exists
        "$ne": None,      # Exclude records where airing_date is null
        "$regex": r"^\d{4}[/\-]\d{2}[/\-]\d{2}$"  # Match 'YYYY/MM/DD' or 'YYYY-MM-DD'
    }

    if start_date and end_date:
        try:
            # start_dt = parse_date(start_date)
            # end_dt = parse_date(end_date)
            print("...start_dt",start_date,"...end_dt",end_date)
            query["airing_date"]["$gte"] = start_date
            query["airing_date"]["$lte"] = end_date
        except ValueError as e:
            print(">>>e",e)
            raise HTTPException(status_code=400, detail=str(e))

    # Filter by genres
    matching_movies = list(db.movie.find(query, {"_id": 1}))
    if not matching_movies:
        return {"movies": [], "total": 0}
    
    matching_movie_ids = [movie["_id"] for movie in matching_movies]
    if genres:
        genre_filter_query = {
            "genres": {"$in": [ObjectId(single_genre) for single_genre in genres]},
            "movie_id": {"$in": matching_movie_ids}  # Ensure movies match the valid date range
        }

        # Retrieve only the drama_ids that match the genre filter and valid dates
        filtered_genres = db.movie_extra_info.find(genre_filter_query, {"movie_id": 1})

        # Update the matching drama IDs to include only those with valid genres
        matching_movie_ids = [movie["movie_id"] for movie in filtered_genres]
    # If no dramas match the genres, return an empty result
    if not matching_movie_ids:
        return {"movies": [], "total": 0}

    # Update the query to include only the drama IDs that matched both the date range and genres
    query["_id"] = {"$in": matching_movie_ids}


    print(">>>>>query",query)

    sort_direction = ASCENDING if direction == "asc" else DESCENDING
    movies = list(db.movie.find(query).sort(order_by, sort_direction).limit(limit).skip(offset))
    # print(">>>>>movies",movies)
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
        movie["_id"] = str(movie["_id"])
        movie['extra_info'] = extra_info
        # print(">movie",movie)

    if not movies:
        raise HTTPException(status_code=404, detail="No Movie found")
    return {"data": movies,"total_count":db.movie.count_documents(query)}

    



@movie_router.get("/{movie_id}")
def get_movie_by_id(movie_id:str):
    single_movie = db.movie.find_one({'_id':ObjectId(movie_id)})
    if not single_movie:
        raise HTTPException(status_code=404, detail="No Movie found")
    extra_info = db.movie_extra_info.find_one({"movie_id":single_movie['_id']},{"_id":0,"movie_id":0})
    if extra_info.get('genres'):
        genres = list(map(lambda x:x['genre_name'],db.genre.find({'_id':{"$in":extra_info['genres']}},{"_id":0})))
        extra_info['genres'] = genres
    if extra_info.get('directed_bys'):
        directed_bys = list(map(lambda x:{**x,"_id":str(x['_id']),"image":get_person_first_image(x['_id'])},db.person.find({'_id':{"$in":extra_info['directed_bys']}},{"birth_of_date":0,"jobs":0,'other_names':0})))
        extra_info['directed_bys'] = directed_bys
    if extra_info.get('written_bys'):
        written_bys = list(map(lambda x:{**x,"_id":str(x['_id']),"image":get_person_first_image(x['_id'])},db.person.find({'_id':{"$in":extra_info['written_bys']}},{"birth_of_date":0,"jobs":0,'other_names':0})))
        extra_info['written_bys'] = written_bys
    extra_info['casts_info']=[]
    if extra_info.get('casts_ids'):
        cast_of_drama = list(map(lambda x:x['cast_id'],db.cast_of_drama.find({'_id':{'$in':extra_info.get('casts_ids',[])}},{'_id':0,"cast_id":1})))
        main_casts = list(map(lambda x:{**x,"_id":str(x['_id']),"image":get_person_first_image(x['_id'])},db.person.find({'_id':{"$in":cast_of_drama}},{"_id":1,"name":1})))
        extra_info['casts_info']+= main_casts
        extra_info.pop("casts_ids",None)
    if extra_info.get('other_cast_info'):
        cast_of_drama = list(map(lambda x:x['cast_id'],db.cast_of_drama.find({'_id':{'$in':extra_info.get('other_cast_info',[])}},{'_id':0,"cast_id":1})))
        other_casts = list(map(lambda x:{**x,"_id":str(x['_id']),"image":get_person_first_image(x['_id'])},db.person.find({'_id':{"$in":cast_of_drama}},{"_id":1,"name":1})))
        extra_info['casts_info']+= other_casts
        extra_info.pop("other_cast_info",None)
        single_movie['extra_info'] = extra_info
        single_movie['_id'] = str(single_movie['_id'])
    return single_movie