from random import choice
from datetime import datetime
import json
import requests

from sqlalchemy import create_engine, text
from random import randint
from time import sleep

from scraper import InstagramScraper
import conf

urls = [
    'https://www.instagram.com/urlocalplantboy/',
    'https://www.instagram.com/bloomstarofficial/',
    'https://www.instagram.com/houseofplantlovers/',
    'https://www.instagram.com/patchplants/',
    'https://www.instagram.com/planterpotters/',
    'https://www.instagram.com/craigowilliams/',
    'https://www.instagram.com/plantloversdecor/',
    'https://www.instagram.com/momagency/',
    'https://www.instagram.com/plantuniversity/',
    'https://www.instagram.com/plantloversonly/',
    'https://www.instagram.com/darkplantmatter/',
    'https://www.instagram.com/lostinplantopia/',
    'https://www.instagram.com/plantsbybenny/',
    'https://www.instagram.com/thepottedjungle/',
    'https://www.instagram.com/workhardplanthard/',
    'https://www.instagram.com/houseplantclub/',
    'https://www.instagram.com/thesill/',
    'https://www.instagram.com/iplanteven/',
    'https://www.instagram.com/urbanjungleblog/',
    'https://www.instagram.com/hiltoncarter/',
]

def connect_to_rds():
    ''' Connects to RDS and returns connection '''
    engine = create_engine(
            "postgresql://{}:{}@{}/{}".format(
                conf.RDS_user,
                conf.RDS_password,
                conf.RDS_host,
                conf.RDS_db_name,
                )
            )
    conn = engine.connect()
    return conn

def create_tables(drop_table=False):
    ''' Creates post_metric table in RDS database '''
    conn = connect_to_rds()

    if drop_table:
        for table in ['post_metrics', 'page_metrics']:
            try:
                sql = 'DROP TABLE {};'.format(table)
                conn.execute(sql)
                print('table {} dropped'.format(table))
            except:
                continue

    # Create new tables
    post_metrics_sql  = """
            CREATE TABLE post_metrics(
            post_id bigint PRIMARY KEY,
            post_shortcode VARCHAR (50),
            user_id VARCHAR (50),
            post_time TIMESTAMP,
            update_time TIMESTAMP,
            post_likes INT,
            post_comments INT,
            post_media VARCHAR,
            post_is_video BOOLEAN,
            post_caption TEXT,
            post_caption_accessibility TEXT,
            post_url VARCHAR,
            post_label_man INT,
            post_label_predict INT,
            post_unuseable_flag INT,
            posted_flag INT,
            );
            """

    page_metrics_sql  = """
            CREATE TABLE page_metrics(
            user_id VARCHAR (50) PRIMARY KEY,
            username VARCHAR (60),
            update_time TIMESTAMP,
            biography TEXT,
            video_timeline INT,
            follows BIGINT,
            followers BIGINT,
            media_collections INT,
            mutual_followed_by INT,
            saved_media INT
            );
            """
    conn.execute(post_metrics_sql)
    print('table created')
    conn.execute(page_metrics_sql)
    print(conn)

    r = conn.execute('select * from post_metrics;')
    print(r)
    conn.close()

create_tables(drop_table=True)

def add_ig_data_from_urls(urls):
    '''
    Takes in a list of Instagram profile urls and parses post and profile
    information and inserts into database
    urls: list
    '''

    conn = connect_to_rds()

    for url in urls:
        print(url)
        sleep(randint(10,100))
        instagram = InstagramScraper(url)
        post_metrics = instagram.post_metrics()
        page_metrics = instagram.page_metrics()

        # Add post metrics
        update_time = datetime.now().isoformat()
        for metric in post_metrics:
            i_id = str(metric['id'])
            i_shortcode = metric['shortcode']
            i_user_id = str(metric['owner']['id'])
            i_post_time = datetime.fromtimestamp(
                metric['taken_at_timestamp']).isoformat()
            i_likes = int(metric['edge_liked_by']['count'])
            i_comments = int(metric['edge_media_to_comment']['count'])
            i_media = metric['display_url']
            i_video = bool(metric['is_video'])
            i_caption = (metric['edge_media_to_caption']['edges'][0]['node']['text']).replace(':', 'colon')
            # i_caption = ''
            try:
                i_accessibility_caption = metric['accessibility_caption']
            except:
                i_accessibility_caption = ''
            i_post_url = 'https:/www.instagram.com/p/{}'.format(i_shortcode)

            insert_sql = """INSERT INTO post_metrics
                    (post_id, post_shortcode, user_id, post_time, update_time,
                    post_likes, post_comments, post_media, post_is_video,
                    post_caption, post_caption_accessibility, post_url, post_label_man, post_label_predict, post_unuseable_flag, posted_flag)
                    VALUES ({}, '{}', {}, '{}', '{}', {}, {}, '{}', {}, $${}$$, '{}', '{}', {}, {}, {}, {})
                """.format(i_id, i_shortcode, i_user_id, i_post_time,
                            update_time, i_likes, i_comments, i_media, i_video,
                            i_caption, i_accessibility_caption, i_post_url, -1, -1, 0, 0)
            print(insert_sql)
            conn.execute(text(insert_sql))

        # Add page metrics
        user_id = str(page_metrics['id'])
        username = page_metrics['username']
        update_time = datetime.now().isoformat()
        bio = page_metrics['biography']#.replace('\n', '')
        video_timeline = page_metrics['edge_felix_video_timeline']
        follows = page_metrics['edge_follow']
        followers = page_metrics['edge_followed_by']
        media_collections = page_metrics['edge_media_collections']
        mutual_followed_by = page_metrics['edge_mutual_followed_by']
        saved_media = page_metrics['edge_saved_media']


        insert_sql_page = """INSERT INTO page_metrics
                        (user_id, username, update_time, biography , video_timeline, follows, followers,
                        media_collections, mutual_followed_by, saved_media)
                        VALUES ({}, '{}', '{}', $${}$$, {}, {}, {}, {}, {}, {})
                    """.format(user_id, username, update_time, bio, video_timeline, follows, followers,
                               media_collections, mutual_followed_by, saved_media)
        conn.execute(text(insert_sql_page))

    conn.close()

add_ig_data_from_urls(urls)
