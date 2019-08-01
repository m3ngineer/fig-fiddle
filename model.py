import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LogisticRegression
import pickle

def train(data):
    '''
    Train model
    '''
    features = ['post_likes', 'post_comments', 'post_is_video', 'engagement_rate', 'num_#', 'num_@', 'caption_length', 'days_since_post_date', 'ratio_comments_days_posted', 'ratio_likes_days_posted', 'followers']
    X, y = data[features], data['good_post']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

    # logistic model
    model = LogisticRegression()
    model.fit(X_train, y_train)
    scores = cross_val_score(model, X_test, y_test, scoring='f1')

    return model

def save_model(model):
    '''
    Save model to pickle
    '''

    filename = 'data/models/model.sav'
    pickle.dump(model, open(filename, 'wb'))
    print('saved model.')

def predict_labels(data, model=None):
    '''
    Predict suitable posts
    '''

    if model is None:
        filename = 'data/models/model.sav'
        model = pickle.load(open(filename, 'rb'))

    data['labels_predict'] = model.predict(data)
    data.to_csv('data/posts_predicted.csv', index=True)
    return data

if __name__ == "__main__":
    data = pd.read_excel('data/posts-labeled2.xlsx')
    data.set_index('post_id', inplace=True)
    model = train(data)
    save_model(model)

    features = ['post_likes', 'post_comments', 'post_is_video', 'engagement_rate', 'num_#', 'num_@', 'caption_length', 'days_since_post_date', 'ratio_comments_days_posted', 'ratio_likes_days_posted', 'followers']

    predict_labels(data[features])
