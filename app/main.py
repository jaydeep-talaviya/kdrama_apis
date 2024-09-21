from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import person, drama,movie
from .celery import celery_app


app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Can add multiple origins, or use ["*"] to allow all
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

app.include_router(movie.movie_router)
app.include_router(drama.drama_router)
app.include_router(person.person_router)

@app.get("/")
def read_root():
    return {"message": "Welcome to my FastAPI application!"}

@app.get("/start-task/")
async def start_task():
    task = celery_app.send_task('print_message', args=['Hello, World!'])
    return {"task_id": task.id}
