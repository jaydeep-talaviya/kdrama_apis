from fastapi import APIRouter,HTTPException, Query
from app.tasks import get_all_kdrama_once
from app.dependencies.mongo import get_mongo_db
from app.schemas import TotalGenresSchema,TotalCompaniesSchema,TotalDramaSchema
from typing import List,Optional
from pymongo import ASCENDING,DESCENDING
from bson import ObjectId
from app.utilities.common_functions import get_person_first_image
from datetime import datetime

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


@drama_router.get("/",response_model=TotalDramaSchema)
def get_dramas(limit: int = Query(10, gt=0), 
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, min_length=1),
    order_by: Optional[str] = Query("drama_name"),  
    direction: Optional[str] = Query("asc"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    genres: Optional[List[str]] = Query(None),
    tv_channels: Optional[List[str]] = Query(None)
):
    query = {}
   
    if search:
         query = {
            "$or": [
                {"drama_name": {"$regex": search, "$options": "i"}},
                {"other_names": {"$regex": search, "$options": "i"}}
            ]
        }
    print(">>>>>>start_date",start_date,"end_date",end_date,"genres",genres,"tv_channels",tv_channels)
    # Filter by date range if start_date and end_date are provided
    date_regex = r"^\d{4}/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])$"
    query["airing_dates_start"] = {"$regex": date_regex, "$type": "string"}
    query["airing_dates_end"] = {"$regex": date_regex, "$type": "string"}


    # Filter by date range if provided (only for valid formats)
    if start_date and end_date:
        try:
            # Ensure start_date and end_date are properly formatted in "YYYY-MM-DD"
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

            query["$expr"] = {
                "$and": [
                    {
                        "$gte": [
                            {"$dateFromString": {
                                "dateString": "$airing_dates_start",
                                "format": "%Y/%m/%d"
                            }},
                            {"$dateFromString": {
                                "dateString": start_date,
                                "format": "%Y-%m-%d"
                            }}
                        ]
                    },
                    {
                        "$lte": [
                            {"$dateFromString": {
                                "dateString": "$airing_dates_end",
                                "format": "%Y/%m/%d"
                            }},
                            {"$dateFromString": {
                                "dateString": end_date,
                                "format": "%Y-%m-%d"
                            }}
                        ]
                    }
                ]
            }


        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format for start_date or end_date")


    # Filter by genres
    matching_dramas = list(db.drama.find(query, {"_id": 1}))
    if not matching_dramas:
        return {"dramas": [], "total": 0}
    
    matching_drama_ids = [drama["_id"] for drama in matching_dramas]
    if genres:
        genre_filter_query = {
            "genres": {"$in": [ObjectId(single_genre) for single_genre in genres]},
            "drama_id": {"$in": matching_drama_ids}  # Ensure dramas match the valid date range
        }
        print(">>>>>>>>>>step 1",genre_filter_query,"\n\n")

        # Retrieve only the drama_ids that match the genre filter and valid dates
        filtered_genres = db.drama_extra_info.find(genre_filter_query, {"drama_id": 1})

        # Update the matching drama IDs to include only those with valid genres
        matching_drama_ids = [drama["drama_id"] for drama in filtered_genres]
        print(">>>>>>>>>>step 2",matching_drama_ids,"\n\n")
    # If no dramas match the genres, return an empty result
    if not matching_drama_ids:
        return {"dramas": [], "total": 0}

    # Update the query to include only the drama IDs that matched both the date range and genres
    query["_id"] = {"$in": matching_drama_ids}

    # Apply TV channel filter if provided
    if tv_channels:
        query["tv_channel_id"] = {"$in": [ObjectId(channel) for channel in tv_channels]}


   
    sort_direction = ASCENDING if direction == "asc" else DESCENDING
    dramas = list(db.drama.find(query).sort(order_by, sort_direction).limit(limit).skip(offset))
    print(">>>>drama",dramas)
    for drama in dramas:
        if drama.get('tv_channel_id'):
            tv_channel = db.tv_channel.find_one({'_id':drama.get('tv_channel_id')},{'_id':0})
            drama['tv_channel'] = tv_channel.get('tv_channel')
        # extra info
        extra_info = db.drama_extra_info.find_one({"drama_id":drama['_id']},{"_id":0,"drama_id":0,"images":0})
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
        drama["_id"] = str(drama["_id"])
        drama['tv_channel_id'] = str(drama['tv_channel_id'])

        drama['extra_info'] = extra_info
        #print(">drama",drama)

    if not dramas:
        raise HTTPException(status_code=404, detail="No Drama found")
    return {"data": dramas,"total_count":db.drama.count_documents(query)}


@drama_router.get("/get_random")
def get_random_kdrama():
    random_images_of_drama = db.drama.aggregate([{ "$match": 
            { "airing_dates_start": { "$gt": "2016/12/31" } } },
            { "$sample": { "size": 20 } },
            { "$project": { "image_url": 1, "_id": 0 } }])
    random_images_of_drama = list(map(lambda x:x['image_url'],random_images_of_drama))
    random_images_of_movie = db.movie.aggregate([{ "$match": 
            { "airing_date": { "$gt": "2016/12/31" } } },
            { "$sample": { "size": 20 } },
            { "$project": { "image_url": 1, "_id": 0 } }])
    random_images_of_movie = list(map(lambda x:x['image_url'],random_images_of_movie))
    
    return {'drama':random_images_of_drama,'movie':random_images_of_movie}


@drama_router.get("/{drama_id}")
def get_drama_by_id(drama_id:str):
    single_drama = db.drama.find_one({'_id':ObjectId(drama_id)})
    if single_drama.get('tv_channel_id'):
        tv_channel = db.tv_channel.find_one({'_id':single_drama.get('tv_channel_id')},{'_id':0})
        single_drama['tv_channel'] = tv_channel.get('tv_channel')
    if not single_drama:
        raise HTTPException(status_code=404, detail="No Drama found")
    extra_info = db.drama_extra_info.find_one({"drama_id":single_drama['_id']},{"_id":0,"drama_id":0})
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
        single_drama['extra_info'] = extra_info
        single_drama['tv_channel_id'] = str(single_drama['tv_channel_id'])

        single_drama['_id'] = str(single_drama['_id'])
    print(single_drama)
    return single_drama


