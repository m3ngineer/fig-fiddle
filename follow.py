from InstagramAPI import InstagramAPI
import pandas as pd

import conf


class GetFollowers():

    def __init__(self):
        self.instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
        self.instaAPI.login()

    def get_following(username_id):
        ''' gets the Followers for a given account '''

        results = self.getUserFollowers(username_id, maxid='')

        return results

    def follow_users(user_ids):
        ''' Follow random sample of users from list '''

        for user_id in user_ids:
            self.instaAPI.follow(user_id)
            
