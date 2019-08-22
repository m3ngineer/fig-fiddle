from InstagramAPI import InstagramAPI
import pandas as pd

from db import connect_to_rds
import conf

class GetFollowers():
    ''' Returns followers for a list of users '''

    def __init__(self):
        self.instaAPI = InstagramAPI(conf.insta_handle, conf.insta_password)
        self.instaAPI.login()
        self.followers = []

    def get_following(self, username_id, max_pages=1, max_id=''):
        ''' gets the Followers for a given account '''

        next_max_id = ''
        i = 0

        print('test')
        while i <= max_pages:
            i+= 1
            self.instaAPI.getUserFollowers(username_id, maxid=next_max_id)
            temp = self.instaAPI.LastJson

            for item in temp['users']:
                self.followers.append(item)

            if temp["big_list"] is False:
                return self.followers

            next_max_id = temp["next_max_id"]

        return self.followers

    def follow_users(self, user_ids):
        ''' Follow random sample of users from list '''

        for user_id in user_ids:
            self.instaAPI.follow(user_id)

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
    print(gf.followers)
    followers = gf.get_following('3519515154')
    print(followers)
    gf.follow_users(followers[:10])
