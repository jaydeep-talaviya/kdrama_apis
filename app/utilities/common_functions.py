import requests
from bs4 import BeautifulSoup
import nest_asyncio
from requests_html import HTMLSession
from datetime import datetime,timedelta
from app.dependencies.mongo import get_mongo_db


db = get_mongo_db()

def get_person_first_image(person_id):
    person_image = db.person_images.find_one({'person_id':person_id},{'image_links':1,"_id":0})
    return person_image['image_links'][0] if person_image else ""
