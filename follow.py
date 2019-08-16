from InstagramAPI import InstagramAPI
import pandas as pd

import conf


class GetFollowers():

    def __init__(self):
        self.instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
        self.instaAPI.login()

    def get_following(self, username_id, max_id=''):
        ''' gets the Followers for a given account '''

        results = self.instaAPI.getUserFollowers(username_id, maxid='')
#         while 1:
#             self.getUserFollowers(usernameId, next_max_id)
#             temp = self.LastJson

#             for item in temp["users"]:
#                 followers.append(item)

#             if temp["big_list"] is False:
#                 return followers
#             next_max_id = temp["next_max_id"]

        return results

    def follow_users(self, user_ids):
        ''' Follow random sample of users from list '''

        for user_id in user_ids:
            self.instaAPI.follow(user_id)
