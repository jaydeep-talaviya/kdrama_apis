from fastapi import APIRouter


movie_router = APIRouter(
    prefix="/movie",  # This is the route prefix
    tags=["movie"],   # Tags help categorize routes in the API docs
)

@movie_router.get("/")
def get_movies():
    return {"message":"Get all Movies"}
