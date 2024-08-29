# celery.py
from celery import Celery
from celery.schedules import crontab

# Create a Celery instance
celery_app = Celery('tasks', broker='redis://localhost:6379/0')

# Optional configuration
celery_app.conf.update(
    result_backend='redis://localhost:6379/0',
    accept_content=['json'],
    task_serializer='json',
    result_serializer='json',
    timezone='UTC',
)

# Specify the Celery Beat schedule


celery_app.conf.beat_schedule = {

    'Get-All-Genres-Daily': {
        'task': 'get_all_genres_everyday',
        'schedule': crontab(hour=22, minute=30),
        'args': ()
    },
    'Get-All-Companies-Daily': {
        'task': 'get_all_company_everyday',
        'schedule': crontab(hour=23, minute=30),
        'args': ()
    },
    'Get-All-Person-Daily': {
        'task': 'get_new_person_everyday',
        'schedule':crontab(hour=23, minute=30),
        'args': ()
    },
    'Get-All-Kdrama-Daily': {
        'task': 'get_new_kdrama_everyday',
        'schedule': crontab(hour=1, minute=00),
        'args': ()
    },
    'Get-All-Movie-Daily': {
        'task': 'get_new_movie_everyday',
        'schedule': crontab(hour=2, minute=30),
        'args': ()
    },
    
        'Update-All-Drama-Daily': {
        'task': 'update_kdrama_everyday',
        'schedule': crontab(hour=4, minute=00),
        'args': () 
    },
        'Update-All-Movie-Daily': {
        'task': 'update_movie_everyday',
        'schedule': crontab(hour=6, minute=00),
        'args': () 
    },
    
}


# Automatically discover tasks in the 'app' package
celery_app.autodiscover_tasks(['app'])