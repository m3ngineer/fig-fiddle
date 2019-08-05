from sqlalchemy import create_engine

import conf

def connect_to_rds(return_engine=False):
    ''' Connects to RDS and returns connection '''
    engine = create_engine(
            "postgresql://{}:{}@{}/{}".format(
                conf.RDS_user,
                conf.RDS_password,
                conf.RDS_host,
                conf.RDS_db_name,
                )
            )
    if return_engine:
        return engine

    conn = engine.connect()
    return conn

def create_tables(drop_table=False):
    ''' Creates post_metric table in RDS database '''
    conn = connect_to_rds()

    if drop_table:
        for table in ['post_metrics', 'page_metrics']:
            try:
                sql = 'DROP TABLE {};'.format(table)
                conn.execute(sql)
                print('table {} dropped'.format(table))
            except:
                continue

    # Create new tables
    post_metrics_sql  = """
            CREATE TABLE post_metrics(
            post_id bigint PRIMARY KEY,
            post_shortcode VARCHAR (50),
            user_id VARCHAR (50),
            post_time TIMESTAMP,
            update_time TIMESTAMP,
            post_likes INT,
            post_comments INT,
            post_media VARCHAR,
            post_is_video BOOLEAN,
            post_caption TEXT,
            post_caption_accessibility TEXT,
            post_url VARCHAR,
            post_label_man INT,
            post_label_predict INT,
            post_unuseable_flag INT,
            posted_flag INT,
            );
            """

    page_metrics_sql  = """
            CREATE TABLE page_metrics(
            user_id VARCHAR (50) PRIMARY KEY,
            username VARCHAR (60),
            update_time TIMESTAMP,
            biography TEXT,
            video_timeline INT,
            follows BIGINT,
            followers BIGINT,
            media_collections INT,
            mutual_followed_by INT,
            saved_media INT
            );
            """
    conn.execute(post_metrics_sql)
    conn.execute(page_metrics_sql)
    print('post_metrics, page_metrics tables created.')
    conn.close()
