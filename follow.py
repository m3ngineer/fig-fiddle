from InstagramAPI import InstagramAPI
import pandas as pd
from time import sleep

from db import connect_to_rds
import conf

class GetFollowers():
    ''' Returns followers for a list of users '''

    def __init__(self):
        self.user_id = '16389780544'
        self.instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
        self.instaAPI.login()
        self.followers = []
        # Add variable holding current followers
        self.self_followers = self.get_following(self.user_id)

    def get_following(self, username_id, max_pages=1, max_id=''):
        ''' gets the Followers for a given account '''

        next_max_id = ''
        i = 0
        results = []

        print('adding users...')
        while i <= max_pages:
            i+= 1
            self.instaAPI.getUserFollowers(username_id, maxid=next_max_id)
            temp = self.instaAPI.LastJson

            for item in temp['users']:
                results.append(item)

            if temp["big_list"] is False:
                followers = [str(user_info['pk']) for user_info in results]
                self.followers = followers

                return self.followers

            next_max_id = temp["next_max_id"]

        return self.followers

    def follow_users(self, user_ids):
        ''' Follow random sample of users from list '''

        print('user_ids: {}'.format(user_ids))
        i = 0
        for user_id in user_ids:
            i += 1
            sleep(2)
            if user_id not in self.self_followers and i <= 5:
                print('Following user {}'.format(user_id))
                self.instaAPI.follow(user_id)
                print('Followed user {}'.format(user_id))

    def unfollow_users(self, num_user_to_unfollow=10):
        ''' Unfollow random sample of users from following '''

        self.following = self.instaAPI.getSelfFollowing()

        for user_id in self.following:
            self.instaAPI.unfollow(user_id)

def get_list_of_users():
    ''' Returns list of user ids from database '''

    conn = connect_to_rds()
    sql = '''
    SELECT DISTINCT user_id FROM page_metrics
    '''

    r = conn.execute(sql).fetchall()
    r = [item[0] for item in r]

    return r

if __name__ == "__main__":

    # print(get_list_of_users())

    gf = GetFollowers()
    followers = gf.get_following('3519515154', max_pages=2)
    print(followers)

    gf.follow_users(followers[:2])
