import random
from db import connect_to_rds

from time import sleep, strftime
from random import randint
import pandas as pd
import os
from InstagramAPI import InstagramAPI
import requests

import conf

def create_post():
    '''
    Create a post object and post to instagram
    '''

    # Select a post from database
    post = select_post()

    # Save media from post
    img_path = 'images/image.jpg'
    img_url = post['post_media']
    img_data = requests.get(img_url).content
    with open(img_path, 'wb') as handler:
        handler.write(img_data)

    # Generate caption
    caption = generate_caption(post)

    # Post to instagram
    instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
    print('logging into instagram...')
    instaAPI.login()
    sleep(5)
    print('creating post...')
    instaAPI.uploadPhoto(img_path, caption=caption)
    print('post uploaded')

    # Update post flag
    post_id = post['post_id']
    update_post_flag(post_id)
    print('database updated.')

def select_post():
    '''
    Select a post from database that hasn't been used yet
    Return: a dataframe of information for selected post
    '''

    engine = connect_to_rds(return_engine=True)
    query = """
        SELECT * from post_metrics post
            JOIN page_metrics page
                ON page.user_id=post.user_id
            WHERE post.post_label_predict=1
                AND post.post_unuseable_flag=0
                AND post.posted_flag=0;
        """
    df = pd.read_sql_query(query, engine)
    rand_post_i = random.choice(df.index)
    post = df.iloc[rand_post_i]

    return post

def update_post_flag(post_id):
    '''
    Update post.post_flag in database to 1
    '''
    conn = connect_to_rds()
    query = """
        UPDATE post_metrics
        SET posted_flag = 1
        WHERE post_id = {}
        """.format(post_id)

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
            line = random.choice(all_lines)
            line = (lambda line: line[:-1] if line[-1:] == '\n' else line)(line) # strip out ending newline char
            return line

    def generate_hashtags():
        filename = 'hashes.txt'
        with open(filename, 'r') as fh:
            all_lines = fh.readlines()
            hashtag_list = random.sample(all_lines, 10)
            hashtag_list = [hashtag[:-1] if hashtag[-1:] == '\n' else hashtag for hashtag in hashtag_list]
            hashtags = ' '.join(hashtag_list)
            return hashtags

    caption = []

    # Generate tagline
    tagline = generate_tagline()
    caption.append(tagline)

    # Add credits
    username = '@' + profile_data['username']
    caption.append('Credits: {}'.format(username))

    # Add hashtags
    hashtags = generate_hashtags()
    caption.append(hashtags)

    return '\n.\n.\n.\n'.join(caption)

def start_driver():
    chromedriver_path = '/Users/mattheweng/bin/chromedriver' # Change this to your own chromedriver path!
    # driver = webdriver.Chrome(executable_path=chromedriver_path)

    mobile_emulation = { "deviceName": "Nexus 5" }
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options, desired_capabilities=chrome_options.to_capabilities())

    return driver

def login(driver):

    sleep(2)
    driver.get('https://www.instagram.com/accounts/login/?source=auth_switcher')
    sleep(3)

    username = driver.find_element_by_name('username')
    username.send_keys(conf.insta_handle)
    password = driver.find_element_by_name('password')
    password.send_keys(conf.insta_password)

    button_login = driver.find_element_by_xpath('//*[@id="react-root"]/section/main/article/div/div/div/form/div[6]/button/div')
    button_login.click()
    sleep(3)

    notnow = driver.find_element_by_xpath('/html/body/div[3]/div/div/div[3]/button[2]')
    notnow.click() #comment these last 2 lines out, if you don't get a pop up asking about notifications

def make_post(image_path, caption):

    instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
    instaAPI.login()
    sleep(5)
    photo_path = image_path
    print('uploading photo...')
    instaAPI.uploadPhoto(photo_path, caption=caption)

create_post()
