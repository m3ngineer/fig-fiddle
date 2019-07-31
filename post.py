import random
from db import connect_to_rds

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep, strftime
from random import randint
import pandas as pd

import conf

def create_post():
    '''
    Create a post and post to instagram
    '''

    # Select a post from database
    engine = connect_to_rds(engine=True)
    query = """
        SELECT * from post_metrics post
            JOIN page_metrics page
                ON page.user_id=post.user_id
            WHERE post.good_post=1
                AND post.post_flag=0;
        """
    df = pd.read_sql_query(query, engine)
    rand_post_i = random.choice(df.index)
    post = df[rand_post_i]

    # generate a post_object
    media = post['media']
    caption = caption = generate_caption(post)

    # post to instagram
        # connect to instagram
        # post to account

    # Update post flag
    update_post_flag(rand_post_i)

def select_post():
    '''
    Select a post from database that hasn't been used yet
    '''

    # connect to database
    # find post that hasn't been updated yet
    # return post

def update_post_flag(post_index):
    '''
    Update postpost_flag in database to 1
    '''
    conn = connect_to_rds()
    query = """
        UPDATE post_metrics
        SET posted_flag = 1
        WHERE post_index = {}
        """.format(post_index)

    conn.execute(query)
    conn.close()

def generate_caption(profile_data):
    '''
    Randomly returns caption from list and puts together a caption
    '''

    def generate_tagline():
        filename = 'captions.txt'
        with open(filename, 'r') as fh:
            all_lines = fh.readlines()
            return random.choice(all_lines)

    def generate_hastags():
        filename = 'hashes.txt'
        with open(filename, 'r') as fh:
            all_lines = fh.readlines()
            return random.choice(all_lines)

    caption = []

    # Generate tagline
    tagline = generate_tagline()
    caption.append(tagline)

    # Add credits
    caption.append('Credits: {}'.format(profile_data))

    # Add hashtags
    hashtags = generate_hashtags()
    caption.append(hashtags)

    return caption

def login():
    chromedriver_path = '/Users/mattheweng/bin/chromedriver' # Change this to your own chromedriver path!
    driver = webdriver.Chrome(executable_path=chromedriver_path)
    sleep(2)
    driver.get('https://www.instagram.com/accounts/login/?source=auth_switcher')
    sleep(3)

    username = driver.find_element_by_name('username')
    username.send_keys(conf.insta_handle)
    password = driver.find_element_by_name('password')
    password.send_keys(conf.insta_password)

    button_login = driver.find_element_by_xpath('//*[@id="react-root"]/section/main/div/article/div/div[1]/div/form/div[4]/button/div')
    button_login.click()
    sleep(3)

    notnow = driver.find_element_by_xpath('/html/body/div[3]/div/div/div[3]/button[2]')
    notnow.click() #comment these last 2 lines out, if you don't get a pop up asking about notifications

login()
