from fastapi import APIRouter
from app.tasks import get_all_person_once

person_router = APIRouter(
    prefix="/person",  # This is the route prefix
    tags=["person"],   # Tags help categorize routes in the API docs
)

@person_router.post("/create_all_person")
def create_all_person_at_once():
    task = get_all_person_once.delay()
    return {"message":"All Kdrama fetching has been started!"}

@person_router.get("/")
def get_movies():
    return {"message": "get all persons"}
