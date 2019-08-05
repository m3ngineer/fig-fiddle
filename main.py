from random import choice
from datetime import datetime
import json
import requests

from sqlalchemy import create_engine, text
from random import randint
from time import sleep

from scraper import InstagramScraper
from db import connect_to_rds
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

def add_ig_data_from_urls(urls):
    '''
    Takes in a list of Instagram profile urls and parses post and profile
    information and inserts into database
    urls: list
    '''

    for url in urls:
        print(url)
        sleep(randint(10,100))
        instagram = InstagramScraper(url)
        post_metrics = instagram.post_metrics()
        page_metrics = instagram.page_metrics()

def insert_post_metric_data(post_metrics):
    ''' Insert data from url into post_metrics table '''

    conn = connect_to_rds()

    update_time = datetime.now().isoformat()

    for metric in post_metrics:
        i_id = str(metric['id'])

        # Check if post already exists in table
        query = "SELECT post_id FROM post_metrics if post_id='{}'".format(i_id)
        query_post_id = conn.execute(query)

        if not query_post_id.fetchall(): # If ID not found

            # Assign variables
            i_shortcode = metric['shortcode']
            i_user_id = str(metric['owner']['id'])
            i_post_time = datetime.fromtimestamp(
                metric['taken_at_timestamp']).isoformat()
            i_likes = int(metric['edge_liked_by']['count'])
            i_comments = int(metric['edge_media_to_comment']['count'])
            i_media = metric['display_url']
            i_video = bool(metric['is_video'])
            i_caption = (metric['edge_media_to_caption']['edges'][0]['node']['text']).replace(':', 'colon')
            try:
                i_accessibility_caption = metric['accessibility_caption']
            except:
                i_accessibility_caption = ''
            i_post_url = 'https:/www.instagram.com/p/{}'.format(i_shortcode)

            # Insert into table
            try:
                insert_sql = """INSERT INTO post_metrics
                        (post_id, post_shortcode, user_id, post_time, update_time,
                        post_likes, post_comments, post_media, post_is_video,
                        post_caption, post_caption_accessibility, post_url, post_label_man, post_label_predict, post_unuseable_flag, posted_flag)
                        VALUES ({}, '{}', {}, '{}', '{}', {}, {}, '{}', {}, $${}$$, '{}', '{}', {}, {}, {}, {})
                    """.format(i_id, i_shortcode, i_user_id, i_post_time,
                                update_time, i_likes, i_comments, i_media, i_video,
                                i_caption, i_accessibility_caption, i_post_url, -1, -1, 0, 0)
                conn.execute(text(insert_sql))
            except Exception as e:
                print('Unable to insert post {}: {}'.format(post_id, e))
                continue

    conn.close()

def insert_page_metric_data(page_metrics):
    ''' Insert data from url into page_metrics table '''

    user_id = str(page_metrics['id'])

    conn = connect_to_rds()
    # Check if user_id already exists
    query = "SELECT username FROM page_metrics if user_id='{}'".format(user_id)
    query_user_id = conn.execute(query)

    if not query_user_id.fetchall(): # If ID not found
        # Assign variables
        username = page_metrics['username']
        update_time = datetime.now().isoformat()
        bio = page_metrics['biography']#.replace('\n', '')
        video_timeline = page_metrics['edge_felix_video_timeline']
        follows = page_metrics['edge_follow']
        followers = page_metrics['edge_followed_by']
        media_collections = page_metrics['edge_media_collections']
        mutual_followed_by = page_metrics['edge_mutual_followed_by']
        saved_media = page_metrics['edge_saved_media']

        # Insert into table
        insert_sql_page = """INSERT INTO page_metrics
                        (user_id, username, update_time, biography , video_timeline, follows, followers,
                        media_collections, mutual_followed_by, saved_media)
                        VALUES ({}, '{}', '{}', $${}$$, {}, {}, {}, {}, {}, {})
                    """.format(user_id, username, update_time, bio, video_timeline, follows, followers,
                               media_collections, mutual_followed_by, saved_media)

        try:
            conn.execute(text(insert_sql_page))
        except Exception as e:
            raise
    conn.close()


add_ig_data_from_urls(urls)
