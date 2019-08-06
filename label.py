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
from model import predict_labels, update_rds_labels_by_csv
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

    # Create new features
    df = create_features(df)

    df.to_csv(filepath, index=False)

    return df

def preprocess(df):
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
    df['post_label_man'] = -1
    df['post_unuseable_flag'] = flag_posts(df)

    return df

def flag_posts(df):

    df['post_unuseable_flag'] = 0

    # Check for ads
    flagged_phrases = ['link in bio', 'buy now', 'limited time', '#ad']
    df['post_unuseable_flag'] = df['post_caption'].apply(lambda row: any([True if phrase in row else False for phrase in flagged_phrases]))

    return df['post_unuseable_flag'].astype(int)

def assign_labels():
    '''
    Assign labels to any posts that have not been predicted
    '''

    conn = connect_to_rds()
    query = """
        SELECT * from post_metrics post
            JOIN page_metrics page
                ON page.user_id=post.user_id
            WHERE post_label_predict=-1;
    """
    result = conn.execute(query)

    # Preprocess new post data and make predictions
    data = pd.DataFrame(result.fetchall(), columns=r.keys())
    data = preprocess(data)
    labels = predict_labels(data)
    update_rds_labels_by_csv(labels)

    conn.close()

    return

def update_label(post_id, label):
    ''' Reassign the label of a post '''
    return


get_unlabeled_data_csv('data/posts-unlabeled.csv')
