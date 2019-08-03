from random import choice
from datetime import datetime
import json
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from random import randint
from time import sleep
import pandas as pd

from db import connect_to_rds
import conf

def get_unlabeled_data_csv(filepath):
    '''
    Export Instagram posts into CSV for labelling
    '''
    engine = connect_to_rds(return_engine=True)

    query = """
    SELECT * from post_metrics post
        JOIN page_metrics page
            ON page.user_id=post.user_id;
    """

    # execute the query and assign it to a pandas dataframe
    df = pd.read_sql_query(query, engine)

    # Feature engineering
    def days_since_posted(row):
        return (datetime.now() - datetime.strptime(str(row['post_time']), '%Y-%m-%d %H:%M:%S')).days

    df['engagement_rate'] = df['post_likes'] / df['followers']* 100
    df['num_#'] = df['post_caption'].str.count('#')
    df['num_@'] = df['post_caption'].str.count('@')
    df['caption_length'] = df['post_caption'].str.len()
    df['days_since_post_date'] = df.apply(lambda row: days_since_posted(row), axis=1)
    df['ratio_comments_days_posted'] = df['post_comments'] / df['days_since_post_date']
    df['ratio_likes_days_posted'] = df['post_likes'] / df['days_since_post_date']
    df['good_post'] = 0
    df['flag'] = flag_posts(df)
    df.to_csv(filepath, index=False)

    return df

def flag_posts(df):

    df['flag'] = 0

    # Check for ads
    flagged_phrases = ['link in bio', 'buy now', 'limited time', '#ad']
    df['flag'] = df['post_caption'].apply(lambda row: any([True if phrase in row else False for phrase in flagged_phrases]))

    return df['flag'].astype(int)

def update_rds_labels_by_csv(filepath):

    read.csv()
    pass

# get_unlabeled_data_csv('data/posts-unlabeled.csv')

engine = connect_to_rds(return_engine=True)
q = """ALTER TABLE post_metrics
ADD flag int;"""

# Upload csv into RDS
# labels = pd.read_csv('data/posts_predicted.csv')
# labels.to_sql(con=engine, name='labels_predict', if_exists='replace')

# Update post_metrics table
q = """
    UPDATE post_metrics
    SET    good_post = CAST(labels_predict.labels_predict AS int)
    FROM labels_predict
    WHERE CAST(post_metrics.post_id AS bigint) = CAST(labels_predict.post_id AS bigint)
    """
conn = connect_to_rds()
conn.execute(q)

q = """select post_id from post_metrics"""
r = conn.execute(q)
print(r.keys())
print(r.fetchall())
