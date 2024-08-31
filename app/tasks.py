from celery import Celery
from .celery import celery_app
from .cron_functions import (get_genre_list, get_companies_list,
                            get_kdrama_links_all, get_person_links_all, 
                            get_movies_links_all, 
                            update_single_drama_info, update_single_movie_info)
from .helper_functions import get_new_person_from_url,get_single_drama_info, get_single_movie_info, is_date
from .models import *
from datetime import datetime, timedelta
import asyncio
import concurrent.futures
from app.dependencies.mongo import get_mongo_db

db=get_mongo_db()

@celery_app.task(name='print_message')
def print_message(message, *args, **kwargs):
    print(f"Celery is working!! Message is {message}")

@celery_app.task(name = "get_all_genres_everyday")
def get_new_genre(*args, **kwargs):
    base_url = 'https://www.hancinema.net/all_korean_movies_dramas.php'
    get_genre_list(base_url)
    print(f"All The Genres are Fetched!")


@celery_app.task(name = "get_all_company_everyday")
def get_new_companies(*args, **kwargs):
    base_url = 'https://www.hancinema.net/korean_entertainment_companies.php'
    get_companies_list(base_url)
    print(f"All The Companies are Fetched!")

@celery_app.task(name = "get_new_person_everyday")
def get_new_person(*args, **kwargs):
    dynamic_url='https://www.hancinema.net/search_korean_people.php?sort=recently_added'
    previous_date=datetime.today()-timedelta(days=7)
    today=datetime.today()
    base_url='https://www.hancinema.net'
    
    get_all_links=get_person_links_all(base_url,dynamic_url,previous_date,today)

    def process_single_person(single_person):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_new_person_from_url(base_url, base_url + "/" + single_person)
            loop.close()  # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_person}: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_person, get_all_links)
    
    print(f"Got The all New Person from last 7 days!")


@celery_app.task(name = "get_all_person_once")  # do run manually
def get_all_person_once(*args, **kwargs):
    dynamic_url='https://www.hancinema.net/search_korean_people.php'
    base_url='https://www.hancinema.net'
    
    get_all_links=get_person_links_all(base_url,dynamic_url)

    def process_single_person(single_person):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_new_person_from_url(base_url, base_url + "/" + single_person)
            loop.close()  # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_person}: {e}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_person, get_all_links)
    
    print(f"Got The all Person!")


@celery_app.task(name = "get_new_kdrama_everyday")
def get_new_upcomming_kdrama(*args, **kwargs):
    dynamic_url = 'https://www.hancinema.net/upcoming-korean-dramas.php'
    base_url = 'https://www.hancinema.net'
    
    # Fetch all K-drama links
    total_new_links = get_kdrama_links_all(base_url, dynamic_url)
    print("Total Links:", len(total_new_links))
    
    def process_single_drama(single_drama):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_single_drama_info(base_url, single_drama)
            loop.close()                     # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_drama}: {e}")

    # Use ThreadPoolExecutor to process each drama concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_drama, total_new_links)
    
    print("Got all upcomming kdrama...!")
    
    return total_new_links


@celery_app.task(name = "get_all_kdrama_once")
def get_all_kdrama_once(*args, **kwargs):
    dynamic_url = 'https://www.hancinema.net/all_korean_dramas.php'
    base_url = 'https://www.hancinema.net'
    
    # Fetch all K-drama links
    total_new_links = get_kdrama_links_all(base_url, dynamic_url)
    print("Total Links:", len(total_new_links))
    
    def process_single_drama(single_drama):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_single_drama_info(base_url, single_drama)
            loop.close()                     # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_drama}: {e}")

    # Use ThreadPoolExecutor to process each drama concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_drama, total_new_links)
    
    print("Got all kdrama at once...!")
    
    return total_new_links


@celery_app.task(name = "get_new_movie_everyday")
def get_all_movie(*args, **kwargs):
    dynamic_url = 'https://www.hancinema.net/upcoming-korean-movies.php'
    base_url = 'https://www.hancinema.net'
    
    # Fetch all movie links
    total_new_links = get_movies_links_all(base_url, dynamic_url)
    print("Total Links:", len(total_new_links))
    
    def process_single_movie(single_movie):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_single_movie_info(base_url, base_url + "/" + single_movie)
            loop.close()                     # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_movie}: {e}")

    # Use ThreadPoolExecutor to process each movie concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_movie, total_new_links)
    
    print("Got all the movies!")



@celery_app.task(name = "get_all_movie_once")
def get_all_movie_once(*args, **kwargs):
    dynamic_url = 'https://www.hancinema.net/all_korean_movies.php'
    base_url = 'https://www.hancinema.net'
    
    # Fetch all K-drama links
    total_new_links = get_movies_links_all(base_url, dynamic_url)
    print("Total Links:", len(total_new_links))
    
    def process_single_movie(single_movie):
        try:
            loop = asyncio.new_event_loop()  # Create a new event loop
            asyncio.set_event_loop(loop)     # Set it as the current event loop
            get_single_movie_info(base_url, base_url + "/" + single_movie)
            loop.close()                     # Close the loop after processing
        except Exception as e:
            print(f"Error processing {single_movie}: {e}")

    # Use ThreadPoolExecutor to process each drama concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_single_movie, total_new_links)
    
    print("Got all the dramas!")




@celery_app.task(name = "update_kdrama_everyday")
def update_kdrama(*args, **kwargs):
    base_url='https://www.hancinema.net'
    updatable_kdrama=db.drama.find()
    for i in updatable_kdrama:
        start_date =is_date(i.get('airing_dates_start'))
        end_date = is_date(i.get('airing_dates_end')) if i.get('airing_dates_end') else False
        if not start_date or (i.get('airing_dates_end') != False and not end_date):
            print(base_url,i.get("drama_link"))
            update_single_drama_info(base_url,i.get("drama_link"))

@celery_app.task(name = "update_movie_everyday")
def update_movie(*args, **kwargs):
    base_url='https://www.hancinema.net'
    updatable_movie=db.movie.find()
    for i in updatable_movie:
        result=is_date(i.get('airing_date'))
        if not result:
            print(base_url,i.get('movie_link'))
            update_single_movie_info(base_url,i.get('movie_link'))