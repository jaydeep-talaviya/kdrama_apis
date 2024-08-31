from bs4 import BeautifulSoup
import nest_asyncio
from requests_html import HTMLSession
from datetime import datetime,timedelta
from dateutil.parser import parse
from app.dependencies.mongo import get_mongo_db

db=get_mongo_db()

def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


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
    print(single_person_url)
    nest_asyncio.apply()
    session = HTMLSession()
    main_url=single_person_url
    try:
        r = session.get(main_url)
        html_str = r.text
        soup = BeautifulSoup(html_str, 'html.parser')
        main_div=soup.find('main',attrs={'class':'main'})
        name=main_div.find('h1',attrs={'itemprop':'name'}).text
        old_person=db.person.find_one({"name":name})
        person_d={'name':name}
        
        if not old_person:
            # images
            image_links=[]
            try:
                image_main_div=main_div.find('div',attrs={'class':'box main_image person'})
                if image_main_div:
                    image_page_link_h4=image_main_div.find('h4')
                    image_page_link=image_page_link_h4.find('a').attrs['href']
                    image_links=get_image_of_single_actor(base_url+"/"+image_page_link)
                
            except Exception as e:
                pass
            # main_name
            main_info_div=main_div.find('div',attrs={'class':'box work_info'})
            
            try:
                main_gender=main_div.find('span',attrs={'itemprop':'gender'}).text    
                if main_gender:
                    person_d['gender']=main_gender
            except Exception as e:
                pass
            try:
                main_bod=main_div.find('span',attrs={'itemprop':'birthDate'}).text
                if main_bod:
                    person_d['birth_of_date']=main_bod
            except Exception as e:
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
                pass
            # filmography
            
            person = db.person.insert_one(person_d)
            if image_links:
                person_images = db.person_images.insert_one({'image_links':image_links,'person_id':person.inserted_id})

            return person.inserted_id
        else:
            return old_person.get('_id')
    except Exception as e:
        print(">>>>>>e",e,"not found ",main_url)


# Drama Helper functions

def get_or_save_director(base_url,list_of_director_tags):
    directed_by=list(map(lambda x:x.text,list_of_director_tags))
    directer_person_link=list(map(lambda x:base_url+"/"+x.attrs['href'],list_of_director_tags))
    directors=[]
    for i in range(0,len(directed_by)):
        single_director=db.person.find_one({"name":directed_by[i]})
        if not single_director:
            new_director_id=get_new_person_from_url(base_url,directer_person_link[i])
            directors.append(new_director_id)
        else:
            directors.append(single_director['_id'])
    return directors


# add writer or get if exist  table -> drama_writer
def get_or_save_writer(base_url,list_of_writer_tags):
    written_by=list(map(lambda x:x.text,list_of_writer_tags))
    written_person_link=list(map(lambda x:base_url+"/"+x.attrs['href'],list_of_writer_tags))
    writters=[]
    for i in range(0,len(written_by)):
        single_writer=db.person.find_one({"name":written_by[i]})
        if not single_writer:
            new_writer_id=get_new_person_from_url(base_url,written_person_link[i])
            writters.append(new_writer_id)
        else:
            writters.append(single_writer['_id'])
    return writters


#main cast for cast_of_drama

def get_or_create_person(cast_name,cast_actors_link):
    single_person = db.person.find_one({"name":cast_name})
    if single_person:
        return single_person['_id']
    else:
        base_url='https://www.hancinema.net'
        return get_new_person_from_url(base_url,cast_actors_link)
    
def get_main_cast_info(cast_actors_names,cast_actors_names_in_drama,cast_actors_links):
    if cast_actors_names and cast_actors_names_in_drama:
        all_casts=[]
        for j in range(0,len(cast_actors_names)):
            single_cast_id= get_or_create_person(cast_actors_names[j],cast_actors_links[j])
            name_in_drama = cast_actors_names_in_drama[j] if cast_actors_names_in_drama[j] !="\n📰 News\n🎥 Credits 📷 Pics\n" else ""
            old_cast= db.cast_of_drama.find_one({'cast_id':single_cast_id,"cast_name_in_drama":name_in_drama})
            if not old_cast:
                castofdrama=db.cast_of_drama.insert_one({'cast_id':single_cast_id,"cast_name_in_drama":name_in_drama,"extended_cast":False})
                all_casts.append(castofdrama.inserted_id)
            else:
                all_casts.append(old_cast["_id"])
        return all_casts

    
    
def add_single_cast(cast_dict,extended):
    cast_name=cast_dict['cast_name']
    cast_role_for_drama=cast_dict['cast_role_for_drama']
    if cast_name !='' or cast_role_for_drama !='':        
        single_cast_id=get_or_create_person(cast_name,cast_dict['cast_link'])
        name_in_drama = cast_role_for_drama if cast_role_for_drama !="\n📰 News\n🎥 Credits 📷 Pics\n" else ""
        old_castof_drama=db.cast_of_drama.find_one({"cast_id":single_cast_id,"cast_name_in_drama":name_in_drama})

        if not old_castof_drama:
            castofdrama=db.cast_of_drama.insert_one({"cast_id":single_cast_id,"cast_name_in_drama":name_in_drama,"extended_cast":extended})
            return castofdrama.inserted_id
        else:
            return old_castof_drama["_id"]
    else:
        return None
    
    
def get_extra_cast_info(base_url,other_casts_link):
    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(other_casts_link)
    html_str = r.text
    
    extra_cast = BeautifulSoup(html_str, 'html.parser')
    all_casts=extra_cast.find('ul',attrs={'class':'list cast'})
    all_casts_list=all_casts.find_all('div',attrs={'class':'work_info_short'})
    all_cast_info=[]
    for single_cast in all_casts_list:
        cast_name=''
        cast_link=''
        cast_role_for_drama=''
        try:
            cast_name=single_cast.find('a').text
        except:
            pass
        try:
            cast_link=single_cast.find('a').attrs['href']
        except:
            pass
        try:
            cast_role_for_drama=single_cast.find_all('p')[-2].text
        except:
            pass
        try:
            cast_role_for_drama+=" ("+single_cast.find_all('p')[-1].text+")"
        except:
            pass
        single_extra_cast=add_single_cast({
            'cast_name':cast_name,
            'cast_link':base_url+"/"+cast_link,
            'cast_role_for_drama':cast_role_for_drama
        },extended=True)
        if single_extra_cast is not None:
            all_cast_info.append(single_extra_cast)
    return all_cast_info


def get_all_images_links(image_page_link):
    nest_asyncio.apply()
    session = HTMLSession()
    r = session.get(image_page_link)
    html_str = r.text
    images_main_source = BeautifulSoup(html_str, 'html.parser')
    image_list_of_single_drama=[]
    try:
        image_content_ul=images_main_source.find('ul',{'class':'list person_list photo_list'})
        image_content_links=image_content_ul.find_all('a',{'data-fancybox':'gallery'})
        image_list_of_single_drama=list(map(lambda x:'https:'+x.attrs['href'],image_content_links))
    except Exception as e:
        print(">>>>error",e,"at ",image_page_link)
    return image_list_of_single_drama


def get_single_drama_info(base_url,single_drama_link):
    print(" started to get Drama ..",single_drama_link)
    try:
        nest_asyncio.apply()
        session = HTMLSession()
        r = session.get(base_url+'/'+single_drama_link)
        html_str = r.text

        soup = BeautifulSoup(html_str,'html.parser')
        main_content_div=soup.find('main',attrs={'class':'main'})
        image_div=main_content_div.find('div',attrs={'class':'main_image_work'})

        main_theme_img='https:'+image_div.find('img').attrs['src']
        main_info_div=main_content_div.find('div',attrs={"class":"work_info"})
        # Name
        drama_name=main_info_div.find('h1').text
        if not db.drama.count_documents({"drama_name":drama_name}):
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
            # Paragraph synopsis
            last_paragraph=[]
            try:
                last_paragraph = list(filter(lambda x: x.find('strong') is None or (
                    x.find('strong') is not None and x.find('strong').text not in ['Directed by', 'Written by',
                                                                                    'Airing dates',
                                                                                    'TV Channel/Platform:']),
                                    synopsis_div.find_all('p')))
                last_paragraph = list(map(str, last_paragraph))
            except Exception as e:
                pass
            #main_casts
            cast_contents=main_content_div.find('div',{'class':'box cast_box'})
            all_casts_div=[]
            try:
                main_casts=cast_contents.find('ul',{'class':'list cast'})
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

    #       create new drama from here
            drama=db.drama.insert_one({"drama_name":drama_name,"image_url":main_theme_img,
                                        "other_names":other_names,"drama_link":str(base_url+'/'+single_drama_link),
                                        "tv_channel_id":tv_channel['_id'] if tv_channel else None,"airing_dates_start":airing_dates_start,
                                        "airing_dates_end":airing_dates_end,"last_paragraph":"".join(last_paragraph)})

            drama_extra_info={"drama_id":drama.inserted_id}
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
            db.drama_extra_info.insert_one(drama_extra_info)
            print(" Finished to get Drama ..", single_drama_link)
        else:
            print("Drama already exist",drama_name)
    except Exception as e:
        print("got some error",e)



def get_single_movie_info(base_url,single_movie_link):
    print(" started to get Movie ..",single_movie_link)
    try:
        nest_asyncio.apply()
        session = HTMLSession()
        r = session.get(single_movie_link)
        html_str = r.text

        soup = BeautifulSoup(html_str,'html.parser')
        main_content_div=soup.find('main',attrs={'class':'main'})
        image_div=main_content_div.find('div',attrs={'class':'main_image_work'})

        #image
        main_theme_img='https:'+image_div.find('img').attrs['src']
        main_info_div=main_content_div.find('div',attrs={"class":"work_info"})
        # Name
        movie_name=main_info_div.find('h1').text
        if not db.movie.count_documents({"movie_name":movie_name}):
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
            last_paragraph=[]
            try:
                last_paragraph = list(filter(lambda x: x.find('strong') is None or (
                    x.find('strong') is not None and x.find('strong').text not in ['Directed by', 'Written by',
                                                                                    'Airing dates',
                                                                                    'TV Channel/Platform:']),
                                                                                    synopsis_div.find_all('p')))
                last_paragraph = list(map(str, last_paragraph))
            except Exception as e:
                pass
            #main_casts
            cast_contents=main_content_div.find('div',{'class':'box cast_box'})
            all_casts_div=[]
            try:
                main_casts=cast_contents.find('ul',{'class':'list cast'})
                all_casts_div=main_casts.find_all('div',{'class':'work_info_short'})
            except:
                pass

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

            movie = db.movie.insert_one({'movie_name':movie_name,'image_url':main_theme_img,
                             "movie_link":str(base_url+'/'+single_movie_link),"other_names":other_names,
                             "airing_date":airing_date,"duration":duration,"last_paragraph":"".join(last_paragraph)})

            movie_extra_info={"movie_id":movie.inserted_id}
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
            db.movie_extra_info.insert_one(movie_extra_info)
        else:
            print("Movie already exist",movie_name)
    except Exception as e:
        print("got some error",e)