import requests
from bs4 import BeautifulSoup
import nest_asyncio
from requests_html import HTMLSession
from datetime import datetime,timedelta
from app.dependencies.mongo import get_mongo_db


db = get_mongo_db()

def get_genre_list(url):
    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(url)
    html_str = r.text
    soup = BeautifulSoup(html_str, 'html.parser')
    selected_all_genre=soup.find('select',attrs={'name':'genre'})
    all_options=selected_all_genre.find_all('option')
    for single_option in list(filter(lambda x:x.text != "-",all_options)):
        if db.genre.count_documents({'genre_name':single_option.text}) == 0:
            db.genre.insert_one({'genre_name':single_option.text})


def get_companies_list(url,next_page=None):
    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(next_page if next_page else url)
    html_str = r.text
    soup = BeautifulSoup(html_str, 'html.parser')
    total_companies = soup.find('ul',attrs={'class':'company_list'})
    companies = total_companies.find_all('a')
    for company in companies:
        if company.text.find('Next ›') == -1 and company.text.find('‹ Previous') == -1:
            link = 'https://www.hancinema.net/'+company.attrs.get('href')
            name = company.text
            
            db.tv_channel.update_one({'tv_channel':name,'tv_channel_link':link},{"$set":{'tv_channel':name,'tv_channel_link':link}},upsert=True)
        elif company.text.find('Next ›') == 0 and company.text.find('‹ Previous') == -1:
            next_page = 'https://www.hancinema.net/'+ company.attrs.get('href')
            get_companies_list(url,next_page)

        

def get_image_of_single_actor(image_page_url):
    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(image_page_url)
    html_str = r.text

    soup = BeautifulSoup(html_str, 'html.parser')
    main_div=soup.find('aside')

    main_ul=main_div.find('ul',attrs={'class':'list person_list photo_list'})
    image_content_links=main_ul.find_all('a',{'data-fancybox':'gallery'})

    image_list_of_single_drama=list(map(lambda x:'https:'+x.attrs['href'],image_content_links))

    return image_list_of_single_drama



def get_new_person_from_url(base_url,single_person_url):
    nest_asyncio.apply()
    session = HTMLSession()
    main_url=single_person_url
    try:
        r = session.get(main_url)
        html_str = r.text
        soup = BeautifulSoup(html_str, 'html.parser')
        main_div=soup.find('main',attrs={'class':'main'})
        name=main_div.find('h1',attrs={'itemprop':'name'}).text
        old_person=db.person.count_documents({"name":name})
        person_d={'name':name}
        
        if not old_person:
            # images
            image_links=[]
            try:
                image_main_div=main_div.find('div',attrs={'class':'box main_image person'})
                if image_main_div:
                    image_page_link_h4=image_main_div.find('h4')
                    print(">>>step 1",image_page_link_h4)
                    image_page_link=image_page_link_h4.find('a').attrs['href']
                    image_links=get_image_of_single_actor(base_url+"/"+image_page_link)
                    print(">>>>>>>",image_links)
                
            except Exception as e:
                print(".>>>>>.error",e)
                pass
            # main_name
            main_info_div=main_div.find('div',attrs={'class':'box work_info'})

            
            try:
                main_gender=main_div.find('span',attrs={'itemprop':'gender'}).text
                if main_gender:
                    person_d['gender']=main_gender
            except:
                pass
            # jobs
            try:
                jobs_list=list(map(lambda x:x.text,main_div.find_all('a',attrs={'itemprop':'jobTitle'})))
                if jobs_list:
                    person_d['jobs']=jobs_list
            except:
                pass
            # other names
            other_names:''
            try:
                other_names=main_div.find('p',attrs={'itemprop':'additionalName'})
                other_names=str(other_names).split(":")[-1].replace("</p>","") if str(other_names) else ''
                if other_names:
                    person_d['other_names']=other_names

            except Exception as e:
                print(">>>\n",e,'\n')
            # filmography
            
            person = db.person.insert_one(person_d)
            if image_links:
                person_images = db.person_images.insert_one({'image_links':image_links,'person_id':person.inserted_id})

            return person
        else:
            return old_person
    except Exception as e:
        print(">>>>>>e",e,"not found ",main_url)
