from fastapi import FastAPI
from app.routers import person, drama,movie

app = FastAPI()

app.include_router(movie.movie_router)
app.include_router(drama.drama_router)
app.include_router(person.person_router)

@app.get("/")
def read_root():
    return {"message": "Welcome to my FastAPI application!"}
