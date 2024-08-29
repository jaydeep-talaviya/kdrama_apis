from fastapi import APIRouter
from app.tasks import get_all_movie_once

movie_router = APIRouter(
    prefix="/movie",  # This is the route prefix
    tags=["movie"],   # Tags help categorize routes in the API docs
)

@movie_router.post("/create_all_movie")
def create_all_movie_at_once():
    task = get_all_movie_once.delay()
    return {"message":"All K-Movie fetching has been started!"}

@movie_router.get("/")
def get_movies():
    return {"message":"Get all Movies"}
