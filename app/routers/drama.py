from fastapi import APIRouter


drama_router = APIRouter(
    prefix="/drama",  # This is the route prefix
    tags=["drama"],   # Tags help categorize routes in the API docs
)

@drama_router.get("/")
def get_dramas():
    return {"message":"Get all dramas"}
