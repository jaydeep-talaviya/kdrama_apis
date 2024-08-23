from fastapi import APIRouter

person_router = APIRouter(
    prefix="/person",  # This is the route prefix
    tags=["person"],   # Tags help categorize routes in the API docs
)

@person_router.get("/")
def get_movies():
    return {"message": "get all persons"}
