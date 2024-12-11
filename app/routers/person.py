from fastapi import APIRouter,Query,HTTPException
from app.tasks import get_all_person_once
from app.schemas.person import TotalPersonSchema
from app.dependencies.mongo import get_mongo_db
from pymongo import ASCENDING,DESCENDING
from typing import Optional,List
from bson import ObjectId
from datetime import datetime
db=get_mongo_db()

person_router = APIRouter(
    prefix="/person",  # This is the route prefix
    tags=["person"],   # Tags help categorize routes in the API docs
)

@person_router.post("/create_all_person")
def create_all_person_at_once():
    task = get_all_person_once.delay()
    return {"message":"All Kdrama fetching has been started!"}

from fastapi import Query, HTTPException
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId

@person_router.get("/", response_model=TotalPersonSchema)
def get_all_persons(limit: int = Query(10, gt=0), 
                    offset: int = Query(0, ge=0),
                    search: Optional[str] = Query(None, min_length=1),
                    order_by: Optional[str] = Query("birth_of_date"),  
                    direction: Optional[str] = Query("desc"),
                    jobs: Optional[List[str]] = Query(None),
                    gender: Optional[str] = Query(None)
                    # start_year: Optional[int] = Query(None),  # Start year filter for birth
                    # end_year: Optional[int] = Query(None)     # End year filter for birth
                    ):
    # Define the initial pipeline
    pipeline = []
    
    # Search query filtering
    if search:
        search_query = {
            "$or": [
                {"tv_channel": {"$regex": search, "$options": "i"}},
                {"name": {"$regex": search, "$options": "i"}},
                {"jobs": {"$regex": search, "$options": "i"}},
                {"other_names": {"$regex": search, "$options": "i"}}
            ]
        }
        pipeline.append({"$match": search_query})
    
    # Jobs filter using $in operator
    if jobs:
        pipeline.append({"$match": {"jobs": {"$in": jobs}}})
        pipeline.append({
            "$addFields": {
                "matched_jobs_count": {
                    "$size": {"$setIntersection": ["$jobs", jobs]}
                }
            }
        })
    
    # Gender filter
    if gender:
        pipeline.append({"$match": {"gender": gender}})

   
    # Sorting by matched jobs count and additional sort field
    sort_direction = ASCENDING if direction == "asc" else DESCENDING
    pipeline.append({
        "$sort": {
            "matched_jobs_count": -1,
            order_by: sort_direction
        }
    })

    # Pagination (limit and skip)
    pipeline.append({"$skip": offset})
    pipeline.append({"$limit": limit})

    # Perform the aggregation pipeline query
    persons = list(db.person.aggregate(pipeline))
        
    # Process the results and fetch images
    for person in persons:
        person["_id"] = str(person["_id"])
        person_image_doc = db.person_images.find_one({"person_id": ObjectId(person["_id"])})

        if person_image_doc and "image_links" in person_image_doc:
            person["person_image"] = person_image_doc["image_links"][0]  # Assuming the first image link

    if not persons:
        raise HTTPException(status_code=404, detail="No Actor/Actress found")
    
    # Get the total count for pagination
    total_count = db.person.count_documents(search_query if search else {})
    
    return {"data": persons, "total_count": total_count}



def get_structure_for_person(collection_name,extra_infos,single_person_id=None):
    collection = db[collection_name]

    for extra_info in extra_infos:
        #movies
        movie_drama = collection.find_one({'_id':extra_info[collection_name+'_id']},{"_id":0,collection_name+'_link':0,"last_paragraph":0,'tv_channel_id':0})
        if movie_drama.get('tv_channel'):
            movie_drama.pop('tv_channel')
        if extra_info.get('genres'):
            extra_info['genres'] = list(db.genre.find({'_id':{"$in":extra_info['genres']}},{'_id':0}))
       
        if extra_info.get(collection_name+'_id'):
            extra_info[collection_name+'_id'] = str(extra_info[collection_name+'_id'])
        if single_person_id:
            movie_drama['name_in_'+collection_name] = db.cast_of_drama.find_one({'$and':[{'cast_id':single_person_id},{'_id':{'$in':extra_info.get('casts_ids',[])+extra_info.get('other_cast_info',[])}}]},{'cast_name_in_drama':1,"extended_cast":1,"_id":0})
            if extra_info.get('casts_ids'):
                del extra_info['casts_ids'] 
            if extra_info.get('other_cast_info'):
                del extra_info['other_cast_info']
        print(">>>>>>>>>>extra_info",movie_drama)

        extra_info[collection_name] = movie_drama

    return extra_infos

@person_router.get("/{person_id}")
def get_person_by_id(person_id: str):
    # Find the person by ID
    single_person = db.person.find_one({'_id': ObjectId(person_id)})
    if not single_person:
        raise HTTPException(status_code=404, detail="No Actor/Actress found")

    # Define the fields to retrieve for movies and dramas
    movie_fields = {"movie_id": 1, "genres": 1, "_id": 0}
    drama_fields = {"drama_id": 1, "genres": 1, "_id": 0}
    print(">>>>>>>>")
    # Directed movies and dramas
    directed_by_movies = list(db.movie_extra_info.find({'directed_bys': {"$in": [ObjectId(person_id)]}}, movie_fields))
    directed_by_dramas = list(db.drama_extra_info.find({'directed_bys': {"$in": [ObjectId(person_id)]}}, drama_fields))

    # Written movies and dramas
    written_of_movies = list(db.movie_extra_info.find({'written_bys': {"$in": [ObjectId(person_id)]}}, movie_fields))
    written_of_dramas = list(db.drama_extra_info.find({'written_bys': {"$in": [ObjectId(person_id)]}}, drama_fields))

    # Cast of dramas and movies
    cast_of_drama_ids = list(db.cast_of_drama.find({'cast_id': {"$in": [ObjectId(person_id)]}}, {'_id': 1}))
    cast_of_drama_ids = [cast_of_drama_id['_id'] for cast_of_drama_id in cast_of_drama_ids]
    cast_of_movies = list(db.movie_extra_info.find(
        {'$or': [{'casts_ids': {"$in": cast_of_drama_ids}}, {'other_cast_info': {"$in": cast_of_drama_ids}}]},
        {**movie_fields, "casts_ids": 1, "other_cast_info": 1}))
    cast_of_dramas = list(db.drama_extra_info.find(
        {'$or': [{'casts_ids': {"$in": cast_of_drama_ids}}, {'other_cast_info': {"$in": cast_of_drama_ids}}]},
        {**drama_fields, "casts_ids": 1, "other_cast_info": 1}))
    print(">>>>>>>>")

    # Structure data for response
    directed_by_movies = get_structure_for_person('movie', directed_by_movies)
    directed_by_dramas = get_structure_for_person('drama', directed_by_dramas)
    cast_of_movies = get_structure_for_person('movie', cast_of_movies, single_person['_id'])
    cast_of_dramas = get_structure_for_person('drama', cast_of_dramas, single_person['_id'])
    written_of_movies = get_structure_for_person('movie', written_of_movies)
    written_of_dramas = get_structure_for_person('drama', written_of_dramas)
    print(">>>>>>>>")

    # Convert person ID to string
    single_person['_id'] = str(single_person['_id'])
    single_person['directed_by_movies'] = directed_by_movies
    single_person['directed_by_dramas'] = directed_by_dramas
    single_person['written_of_movies'] = written_of_movies
    single_person['written_of_dramas'] = written_of_dramas
    single_person['cast_of_movies'] = cast_of_movies
    single_person['cast_of_dramas'] = cast_of_dramas

    # Fetch the person's image from person_images collection
    person_image_doc = db.person_images.find_one({"person_id": ObjectId(person_id)})
    if person_image_doc and "image_links" in person_image_doc:
        single_person['person_images'] = person_image_doc['image_links']  # Include all image links
    print(">>>>>>",single_person)

    return single_person
