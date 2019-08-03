import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LogisticRegression
import pickle

from db import connect_to_rds

def train(data):
    '''
    Train model
    '''
    features = ['post_likes', 'post_comments', 'post_is_video', 'engagement_rate', 'num_#', 'num_@', 'caption_length', 'days_since_post_date', 'ratio_comments_days_posted', 'ratio_likes_days_posted', 'followers']
    X, y = data[features], data['post_labels_man']

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

def predict_labels(data, index='post_id', features=None, model=None):
    '''
    Predict suitable posts
    '''

    if features is None:
        features = ['post_likes', 'post_comments', 'post_is_video', 'engagement_rate', 'num_#', 'num_@', 'caption_length', 'days_since_post_date', 'ratio_comments_days_posted', 'ratio_likes_days_posted', 'followers']
    if model is None:
        filename = 'data/models/model.sav'
        model = pickle.load(open(filename, 'rb'))

    data['post_label_predict'] = model.predict(data[features])
    data[['post_id','post_label_predict']].to_csv('data/posts_predicted.csv', index=False)

    return data[['post_id','post_label_predict']]

def update_rds_labels_by_csv(labels):

    # Upload predicted labels as post_label_predictions table in RDS
    engine = connect_to_rds(return_engine=True)
    labels.to_sql(con=engine, name='post_label_predictions', if_exists='replace')

    # Update post_metrics table with predicted labels
    q = """
        UPDATE post_metrics
        SET    post_label_predict = CAST(post_label_predictions.post_label_predict AS int)
        FROM post_label_predictions
        WHERE CAST(post_metrics.post_id AS bigint) = CAST(post_label_predictions.post_id AS bigint)
        """

    conn = engine.connect()
    conn.execute(q)
    print('labels updated.')

if __name__ == "__main__":
    # Training a model
    # data = pd.read_excel('data/posts-labeled2.xlsx')
    # data.set_index('post_id', inplace=True)
    # model = train(data)
    # save_model(model)

    # Making predictions
    data = pd.read_csv('data/posts-unlabeled.csv')
    labels = predict_labels(data)
    update_rds_labels_by_csv(labels)
