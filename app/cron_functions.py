import requests
from bs4 import BeautifulSoup
import nest_asyncio
from requests_html import HTMLSession
from datetime import datetime,timedelta
from .helper_functions import *
import concurrent.futures
import asyncio
from app.dependencies.mongo import get_mongo_db

db=get_mongo_db()
# genres

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


# Companies

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


# person 

def get_person_links_all(url, main_search_link, previous_date=None, today=None):
    nest_asyncio.apply()

    session = HTMLSession()
    total_kdrama_links = []
    next_link = main_search_link
    while next_link:
        r = session.get(next_link)
        html_str = r.text
        soup = BeautifulSoup(html_str, 'html.parser')
        links_from_genre = []
        main_content_of_links = soup.find('ul', attrs={'class': 'list person_list'})
        links_tag_parent = main_content_of_links.find_all('li')

        # Function to process each link tag
        def process_single_link_tag(single_link_tag):
            try:
                current_datetime = datetime.strptime(single_link_tag.find('a').find('strong').text, '%Y/%m/%d')
                if (not previous_date and not today) or (previous_date and current_datetime >= previous_date and current_datetime <= today):
                    return single_link_tag.find('a').attrs['href']
            except Exception as e:
                print(f">>> Error on {single_link_tag.find('a').attrs['href']}: {e}")
            return None

        # Using ThreadPoolExecutor for concurrent execution of link processing
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_link = {executor.submit(process_single_link_tag, tag): tag for tag in links_tag_parent}
            for future in concurrent.futures.as_completed(future_to_link):
                link = future.result()
                if link:
                    links_from_genre.append(link)
                    print(link)

        total_kdrama_links.extend(links_from_genre)

        # Handling pagination
        page_nxt_btn = main_content_of_links.find_next('nav')
        next_page_links = page_nxt_btn.find_all('a', string="Next ›") if page_nxt_btn else []
        next_link = url + next_page_links[0].attrs['href'] if next_page_links else None

        # To avoid an infinite loop, break if no new links were added in this iteration
        if not links_from_genre:
            break

    return total_kdrama_links


def get_kdrama_links_all(url, main_search_link):
    nest_asyncio.apply()

    session = HTMLSession()
    links_from_genre = []
    next_link = main_search_link

    while next_link:
        print(next_link)
        r = session.get(next_link)
        html_str = r.text
        soup = BeautifulSoup(html_str, 'html.parser')
        main_content_of_links = soup.find('ul', attrs={'class': 'list work_list'})
        links_tag_parent = main_content_of_links.find_all('div', attrs={'class': 'work_info_short'})

        def process_single_link_tag(single_link_tag):
            try:
                airing_dates = single_link_tag.find('span', attrs={'itemprop': 'datePublished'}).text
                return single_link_tag.find('a').attrs['href']
            except Exception as e:
                print(f"Error processing link: {e}")
                return None

        # Use ThreadPoolExecutor to process links concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_link = {executor.submit(process_single_link_tag, tag): tag for tag in links_tag_parent}
            for future in concurrent.futures.as_completed(future_to_link):
                link = future.result()
                if link:
                    links_from_genre.append(link)
                    print(link)

        # Handle pagination
        page_nxt_btn = main_content_of_links.find_next('nav')
        next_page_links = page_nxt_btn.find_all('a', string="Next ›") if page_nxt_btn else []
        next_link = url + next_page_links[0].attrs['href'] if next_page_links else None

        # Break the loop if no new links are found
        if not links_tag_parent:
            break

    return links_from_genre




def get_movies_links_all(url, main_search_link):
    nest_asyncio.apply()

    session = HTMLSession()
    links_from_genre = []
    next_link = main_search_link

    while next_link:
        print("Processing:", next_link)
        r = session.get(next_link)
        html_str = r.text
        soup = BeautifulSoup(html_str, 'html.parser')
        main_content_of_links = soup.find('ul', attrs={'class': 'list work_list'})
        links_tag_parent = main_content_of_links.find_all('div', attrs={'class': 'work_info_short'})

        def process_single_link_tag(single_link_tag):
            try:
                airing_dates = single_link_tag.find('span', attrs={'itemprop': 'datePublished'}).text
                return single_link_tag.find('a').attrs['href']
            except Exception as e:
                print(f"Error processing link: {e}")
                return None

        # Use ThreadPoolExecutor to process links concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_link = {executor.submit(process_single_link_tag, tag): tag for tag in links_tag_parent}
            for future in concurrent.futures.as_completed(future_to_link):
                link = future.result()
                if link:
                    links_from_genre.append(link)

        # Handle pagination
        page_nxt_btn = main_content_of_links.find_next('nav')
        next_page_links = page_nxt_btn.find_all('a', string="Next ›") if page_nxt_btn else []
        next_link = url + next_page_links[0].attrs['href'] if next_page_links else None

        # Break the loop if no new links are found
        if not links_tag_parent:
            break

    return links_from_genre


def update_single_drama_info(base_url,single_drama_link):

    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(single_drama_link)
    html_str = r.text
    
    soup = BeautifulSoup(html_str,'html.parser')
    main_content_div=soup.find('main',attrs={'class':'main'})
    image_div=main_content_div.find('div',attrs={'class':'main_image_work'})

    #image
    main_info_div=main_content_div.find('div',attrs={"class":"work_info"})
    # Name
    drama_name=main_info_div.find('h1').text
    if db.drama.count_documents({"drama_name":drama_name}) == 1:
        
        # other names
        other_names=main_info_div.find('h3').text.split("|")
        # genres
        genres_list=list(map(lambda x:x.text,main_info_div.find_all('a',{'itemprop':'genre'})))
        
        #directed_by
        synopsis_div=main_info_div.find('div',attrs={'class':'synopsis'})
        directed_tag_list=synopsis_div.find_all('a',attrs={'itemprop':'director'})
        directed_by_ids=get_or_save_director(base_url,directed_tag_list)

        # writer by
        written_tag_list=synopsis_div.find_all('a',attrs={'itemprop':'author'})
        written_by_ids=get_or_save_writer(base_url,written_tag_list)

        # TV Channel/Platform:
        tv_channel_list=list(map(lambda x:x.text,synopsis_div.find_all('a',attrs={'itemprop':'provider'})))
        tv_channel_link=list(map(lambda x:base_url+"/"+x.attrs['href'],synopsis_div.find_all('a',attrs={'itemprop':'provider'})))
        tv_channel=None
        if tv_channel_link:
            tv_channel=db.tv_channel.find_one({"tv_channel":tv_channel_list[0]})

        # Airing dates
        airing_dates=list(map(lambda x:x.text,synopsis_div.find_all('span',attrs={'itemprop':'datePublished'})))[0]
        airing_dates_start=airing_dates.split("~")[0]
        airing_dates_end=False
        if len(airing_dates.split("~"))>1:
            airing_dates_end=airing_dates.split("~")[1]
        
        last_paragraph=''
        last_paragraph=''
        try:
            last_paragraph = list(filter(lambda x: x.find('strong') is None or (
                x.find('strong') is not None and x.find('strong').text not in ['Directed by', 'Written by',
                                                                           'Airing dates',
                                                                           'TV Channel/Platform:']),
                                 synopsis_div.find_all('p')))[0]
        except Exception as e:
            pass

        #main_casts
        cast_contents=main_content_div.find('div',{'class':'box cast_box'})
        # main_casts_names
        all_casts_div=[]
        try:
            main_casts=cast_contents.find('ul',{'class':'list cast'})
            # main_casts_names
            all_casts_div=main_casts.find_all('div',{'class':'work_info_short'})
        except:
            pass

        cast_actors_names=[]
        cast_actors_links=[]
        cast_actors_names_in_drama=[]
        try:
            cast_actors_names=list(map(lambda x:x.find('a').text,all_casts_div))
        except:
            cast_actors_names=list(map(lambda x:x.find('i').text,all_casts_div))
        try:
            cast_actors_links=list(map(lambda x:base_url+"/"+x.find('a').attrs['href'],all_casts_div))
        except:
            pass
        try:
            cast_actors_names_in_drama=list(map(lambda x:x.find_all('p')[-1].text,all_casts_div))
        except:
            pass


        cast_actors_names=cast_actors_names
        cast_actors_links=cast_actors_links
        cast_actors_names_in_drama=cast_actors_names_in_drama
        # save into db for castof drama
        casts_ids=get_main_cast_info(cast_actors_names,cast_actors_names_in_drama,cast_actors_links)
        
        # other_casts_names
        other_cast_info=[]
        try:
            other_casts_link=base_url+"/"+cast_contents.find('h4').find('a').attrs['href']
            other_cast_info=get_extra_cast_info(base_url,other_casts_link)
        except:
            pass        
        image_page_link=base_url+"/"+image_div.find('h4').find('a').attrs['href']


        drama_d={'drama_name':drama_name,
                'tv_channel':tv_channel,
                'airing_dates_start':airing_dates_start,
                'airing_dates_end':airing_dates_end,
                'last_paragraph':str(last_paragraph)
                }
        db.drama.update_one({"drama_name":drama_name},{"$set":drama_d})
        drama_extra_info={}
        if genres_list:
            genres = db.genre.find({"genre_name": { "$in": genres_list }},{"_id":1})
            drama_extra_info["genres"]=[i['_id'] for i in genres]
        if directed_by_ids:
            drama_extra_info['directed_bys']=[i for i in directed_by_ids]
        if written_by_ids:
            drama_extra_info['written_bys']=[i for i in written_by_ids]

        if casts_ids:
            drama_extra_info['casts_ids']=casts_ids
        if other_cast_info:
            drama_extra_info['other_cast_info']=other_cast_info

        image_of_single_drama=get_all_images_links(image_page_link)
        drama_extra_info['images']=image_of_single_drama
        current_drama = db.drama.find_one({"drama_name":drama_name},{'_id':1})
        db.drama_extra_info.update_one(current_drama,{"$set":drama_extra_info})
        print("<<<<<updated")


def update_single_movie_info(base_url,single_movie_link):
    
    try:
        nest_asyncio.apply()
        session = HTMLSession()
        r = session.get(single_movie_link)
        html_str = r.text

        soup = BeautifulSoup(html_str,'html.parser')
        main_content_div=soup.find('main',attrs={'class':'main'})
        image_div=main_content_div.find('div',attrs={'class':'main_image_work'})

        #image

        main_info_div=main_content_div.find('div',attrs={"class":"work_info"})
        # Name
        movie_name=main_info_div.find('h1').text
        if db.movie.count_documents({'movie_name':movie_name}) == 1:
            # other names
            other_names=main_info_div.find('h3').text.split("|")
            # genres
            genres_list=list(map(lambda x:x.text,main_info_div.find_all('a',{'itemprop':'genre'})))

            #directed_by
            synopsis_div=main_info_div.find('div',attrs={'class':'synopsis'})
            directed_tag_list=synopsis_div.find_all('a',attrs={'itemprop':'director'})
            directed_by_ids=get_or_save_director(base_url,directed_tag_list)

            # writer by
            written_tag_list=synopsis_div.find_all('a',attrs={'itemprop':'author'})
            written_by_ids=get_or_save_writer(base_url,written_tag_list)

            airing_date=False
            try:
                release=list(filter(lambda x:x.text,synopsis_div.find_all('span',attrs={'itemprop':'datePublished'})))
                if release:
                    airing_date=str(release[0]).replace('<span itemprop="datePublished">','').replace("</span>","")

            except Exception as e:
                pass

            duration=False
            try:
                duration_list=list(filter(lambda x:x.text,synopsis_div.find_all('span',attrs={'itemprop':'duration'})))
                if duration_list:
                    duration=str(duration_list[0]).replace('<span itemprop="duration">','').replace("</span>","")

            except Exception as e:
                pass

            # Paragraph synopsis
            last_paragraph=''
            try:
                last_paragraph = list(filter(lambda x: x.find('strong') is None or (
                    x.find('strong') is not None and x.find('strong').text not in ['Directed by', 'Written by',
                                                                                   'Airing dates',
                                                                                   'TV Channel/Platform:']),
                                     synopsis_div.find_all('p')))[-1]
            except Exception as e:
                pass

            #main_casts
            cast_contents=main_content_div.find('div',{'class':'box cast_box'})
            all_casts_div=[]
            try:
                main_casts=cast_contents.find('ul',{'class':'list cast'})
                # main_casts_names
                all_casts_div=main_casts.find_all('div',{'class':'work_info_short'})
            except:
                pass

            cast_actors_links=[]
            cast_actors_names_in_drama=[]
            try:
        #         cast_actors_names=single_cast.find('a').text
                cast_actors_names=list(map(lambda x:x.find('a').text,all_casts_div))
            except:
                cast_actors_names=list(map(lambda x:x.find('i').text,all_casts_div))
            try:
        #         cast_actors_link=single_cast.find('a').attrs['href']
                cast_actors_links=list(map(lambda x:base_url+"/"+x.find('a').attrs['href'],all_casts_div))

            except:
                pass
            try:
        #         cast_actors_names_in_drama=single_cast.find_all('p')[-2].text
                cast_actors_names_in_drama=list(map(lambda x:x.find_all('p')[-1].text,all_casts_div))

            except:
                pass


            cast_actors_names=cast_actors_names
            cast_actors_links=cast_actors_links
            cast_actors_names_in_drama=cast_actors_names_in_drama

            # save into db for castof drama
            casts_ids=get_main_cast_info(cast_actors_names,cast_actors_names_in_drama,cast_actors_links)


            # other_casts_names
            other_cast_info=[]
            try:
                other_casts_link=base_url+"/"+cast_contents.find('h4').find('a').attrs['href']
                other_cast_info=get_extra_cast_info(base_url,other_casts_link)
            except:
                pass        # all_images_for_sintotal_action_dramagle_drama
            image_page_link=base_url+"/"+image_div.find('h4').find('a').attrs['href']


    #         update new movie from here
            movie_d={"movie_name":movie_name,"other_names":other_names,
                    "airing_date":airing_date,'duration':duration,
                    'last_paragraph':str(last_paragraph)}
            movie = db.movie.update_one({"movie_name":movie_name},{"$set":movie_d})

    #         print("step 2",current_movie.airing_date)

            movie_extra_info={}
            if genres_list:
                genres = db.genre.find({"genre_name": { "$in": genres_list}},{"_id":1})
                movie_extra_info["genres"]=[i['_id'] for i in genres]
            if directed_by_ids:
                movie_extra_info['directed_bys']=[i for i in directed_by_ids]
            if written_by_ids:
                movie_extra_info['written_bys']=[i for i in written_by_ids]
            if casts_ids:
                movie_extra_info['casts_ids']=casts_ids
            if other_cast_info:
                movie_extra_info['other_cast_info']=other_cast_info
            image_of_single_drama=get_all_images_links(image_page_link)
            movie_extra_info['images']=image_of_single_drama
            current_movie = db.movie.find_one({'movie_name':movie_name},{'_id':1})
            db.movie_extra_info.update_one(current_movie,{"$set":movie_extra_info})
            print(">>>>updated")
    except Exception as e:
        print("...movie got error",e)