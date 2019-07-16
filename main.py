from random import choice
from datetime import datetime
import json
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

import conf

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36']


class InstagramScraper:

    def __init__(self, url, user_agents=None, proxy=None):
        self.url = url
        self.user_agents = user_agents
        self.proxy = proxy

    def __random_agent(self):
        if self.user_agents and isinstance(self.user_agents, list):
            return choice(self.user_agents)
        return choice(USER_AGENTS)

    def __request_url(self, url):
        try:
            response = requests.get(
                url,
                headers={'User-Agent': self.__random_agent()},
                proxies={'http': self.proxy, 'https': self.proxy})
            response.raise_for_status()
        except requests.HTTPError:
            raise requests.HTTPError(
                'Received non 200 status code from Instagram')
        except requests.RequestException:
            raise requests.RequestException
        else:
            return response.text

    @staticmethod
    def extract_json(html):
        soup = BeautifulSoup(html, 'html.parser')
        body = soup.find('body')
        script_tag = body.find('script')
        raw_string = script_tag.text.strip().replace(
            'window._sharedData =', '').replace(';', '')
        return json.loads(raw_string)

    def page_metrics(self):
        results = {}
        try:
            response = self.__request_url(self.url)
            json_data = self.extract_json(response)
            metrics = json_data['entry_data']['ProfilePage'][0]['graphql']['user']
        except Exception as e:
            raise e
        else:
            for key, value in metrics.items():
                if key != 'edge_owner_to_timeline_media':
                    if value and isinstance(value, dict):
                        value = value['count']
                        results[key] = value
                    elif key in ['biography', 'id', 'username']:
                        results[key] = value
        return results

    def post_metrics(self):
        results = []
        try:
            response = self.__request_url(self.url)
            json_data = self.extract_json(response)
            metrics = json_data['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']["edges"]
        except Exception as e:
            raise e
        else:
            for node in metrics:
                node = node.get('node')
                if node and isinstance(node, dict):
                    results.append(node)
        return results

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

instagram = InstagramScraper(urls[0])
post_metrics = instagram.post_metrics()
page_metrics = instagram.page_metrics()

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
        sql = 'DROP TABLE post_metrics, page_metrics;'
        conn.execute(sql)

    # Create new table
    post_metrics_sql  = """
            CREATE TABLE post_metrics(
            post_id VARCHAR (50) PRIMARY KEY,
            post_time TIMESTAMP,
            update_time TIMESTAMP,
            post_likes INT,
            post_comments INT,
            post_media VARCHAR,
            post_is_video BOOLEAN
            );
            """
    page_metrics_sql  = """
            CREATE TABLE page_metrics(
            post_id VARCHAR (50) PRIMARY KEY,
            post_time TIMESTAMP,
            update_time TIMESTAMP,
            post_likes INT,
            post_comments INT,
            post_media VARCHAR,
            post_is_video BOOLEAN
            );
            """
    conn.execute(sql)
    print(conn)
    conn.close()

create_tables(drop_table=True)
conn = connect_to_rds()

update_time = datetime.now().isoformat()

for metric in post_metrics[:1]:
    i_id = str(metric['id'])
    i_post_time = datetime.fromtimestamp(
        metric['taken_at_timestamp']).isoformat()
    i_likes = int(metric['edge_liked_by']['count'])
    i_comments = int(metric['edge_media_to_comment']['count'])
    i_media = metric['display_url']
    i_video = bool(metric['is_video'])

    insert_sql = """INSERT INTO post_metrics
                    (post_id, post_time, update_time, post_likes, post_comments, post_media, post_is_video)
                    VALUES ({}, '{}', '{}', {}, {}, '{}', {})
                """.format(i_id, i_post_time, update_time, i_likes, i_comments, i_media, i_video)
    conn.execute(insert_sql)

conn.close()
