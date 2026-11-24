from airflow import DAG
from airflow.operators.python_operator import PythonOperator # Так как мы пишем таски в питоне
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
import pandahouse as ph

# подключение к Clickhouse
connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': '',
    'user': 'student',
    'database': 'simulator_20261020'
}

connection_test = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': '',
    'user': 'student-rw',
    'database': 'test'
}

# параметры DAG
default_args = {
    'owner': 'a.egorova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 11, 20),
}

# Интервал запуска DAG
schedule_interval = '0 04 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_user_activity_egorova():

    @task()
    def extract_feed(event_date):
        # получаем необходимые данные по ленте
        q = """
        SELECT
            user_id,
            toDate('{event_date}') AS event_date,
            os,
            gender,
            age,
            SUM(action='like') AS likes,
            SUM(action='view') AS views
        FROM {db}.feed_actions
        WHERE toDate(time) = toDate('{event_date}')
        GROUP BY user_id, os, gender, age
        """.format(event_date=event_date, db=connection['database'])
        
        feed_tab = ph.read_clickhouse(q, connection=connection)
        return feed_tab
    
    @task()
    def extract_messages(event_date):
        # получаем необходимые данные по месседжеру
        q = """
        SELECT
            t.user_id,
            t.event_date,
            ui.os,
            ui.gender,
            ui.age,
            t.messages_sent,
            t.users_sent,
            t.messages_received,
            t.users_received
        FROM (
            SELECT
                IF(sm.user_id = 0, rm.receiver_id, sm.user_id) AS user_id,
                toDate('{event_date}') AS event_date,
                messages_sent,
                users_sent,
                messages_received,
                users_received
            FROM (
                SELECT user_id, COUNT(receiver_id) AS messages_sent, COUNT(DISTINCT receiver_id) AS users_sent
                FROM {db}.message_actions
                WHERE toDate(time) = toDate('{event_date}')
                GROUP BY user_id, toDate(time)
            ) sm
            FULL OUTER JOIN (
                SELECT receiver_id, COUNT(user_id) AS messages_received, COUNT(DISTINCT user_id) AS users_received
                FROM {db}.message_actions
                WHERE toDate(time) = toDate('{event_date}')
                GROUP BY receiver_id
            ) rm ON sm.user_id = rm.receiver_id
        ) t 
        LEFT JOIN (
            SELECT DISTINCT user_id, os, gender, age
            FROM {db}.message_actions
            
            UNION DISTINCT
            
            SELECT DISTINCT user_id, os, gender, age
            FROM {db}.feed_actions
        ) ui ON ui.user_id = t.user_id
        """.format(event_date=event_date, db=connection['database'])

        message_tab = ph.read_clickhouse(q, connection=connection)  
        return message_tab
    
    @task()
    def merge_user_activity(feed_tab, message_tab):
        merged_df = feed_tab.merge(message_tab, how='outer', on=['user_id', 'event_date', 'os', 'gender', 'age'])
        
        metric_cols = ['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']
        merged_df[metric_cols] = merged_df[metric_cols].fillna(0)
        
        return merged_df
    
    @task()
    def calculate_metrics_by_gender(merged_df):
        metric_cols = ['user_id', 'event_date', 'gender', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']
        
        user_activity_by_gender = merged_df[metric_cols].groupby(['gender', 'event_date'], as_index=False).agg(
            active_users=('user_id', 'nunique'), 
            likes=('likes', 'sum'), 
            views=('views', 'sum'),
            messages_sent=('messages_sent', 'sum'), 
            users_sent=('users_sent', 'sum'), 
            messages_received=('messages_received', 'sum'), 
            users_received=('users_received', 'sum'))
        
        user_activity_by_gender['dimension'] = 'gender'
        
        user_activity_by_gender = user_activity_by_gender.rename(columns={'gender': 'dimension_value'})
        
        user_activity_by_gender['dimension_value'] = (user_activity_by_gender['dimension_value'].astype(str))
        
        return user_activity_by_gender
    
    @task()
    def calculate_metrics_by_age(merged_df):
        metric_cols = ['user_id', 'event_date', 'age', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']
        
        user_activity_by_age = merged_df[metric_cols].groupby(['age', 'event_date'], as_index=False).agg(
            active_users=('user_id', 'nunique'), 
            likes=('likes', 'sum'), 
            views=('views', 'sum'),
            messages_sent=('messages_sent', 'sum'), 
            users_sent=('users_sent', 'sum'), 
            messages_received=('messages_received', 'sum'), 
            users_received=('users_received', 'sum'))
        user_activity_by_age['dimension'] = 'age'
        
        user_activity_by_age = user_activity_by_age.rename(columns={'age': 'dimension_value'})
        
        user_activity_by_age['dimension_value'] = (user_activity_by_age['dimension_value'].astype(str))
        
        return user_activity_by_age
    
    @task()
    def calculate_metrics_by_os(merged_df):
        metric_cols = ['user_id', 'event_date', 'os', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']
        
        user_activity_by_os = merged_df[metric_cols].groupby(['os', 'event_date'], as_index=False).agg(
            active_users=('user_id', 'nunique'), 
            likes=('likes', 'sum'), 
            views=('views', 'sum'),
            messages_sent=('messages_sent', 'sum'), 
            users_sent=('users_sent', 'sum'), 
            messages_received=('messages_received', 'sum'), 
            users_received=('users_received', 'sum'))
        
        user_activity_by_os['dimension'] = 'os'
        
        user_activity_by_os = user_activity_by_os.rename(columns={'os': 'dimension_value'})
        
        user_activity_by_os['dimension_value'] = (user_activity_by_os['dimension_value'].astype(str))
        
        return user_activity_by_os
    
    @task()
    def combine_dimension_metrics(calculate_metrics_by_gender, calculate_metrics_by_age, calculate_metrics_by_os):
        user_activity_metrics = pd.concat([calculate_metrics_by_gender, calculate_metrics_by_age, calculate_metrics_by_os])
        
        user_activity_metrics = user_activity_metrics[[
            'event_date', 'dimension', 'dimension_value',
            'likes', 'views', 'messages_sent', 
            'users_sent', 'messages_received', 'users_received']]
        
        return user_activity_metrics
    
    @task()
    def load(user_activity_metrics, event_date):
        table_name = 'user_activity_metrics_egorova'
        
        create_query = f"""
        CREATE TABLE IF NOT EXISTS test.{table_name}
        (
            event_date Date,
            dimension String,
            dimension_value String,
            likes UInt64,
            views UInt64,
            messages_sent UInt64,
            users_sent UInt64,
            messages_received UInt64,
            users_received UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        
        ph.execute(create_query, connection=connection_test)
        
        delete_query = f"""
        ALTER TABLE test.{table_name}
        DELETE WHERE event_date = toDate('{event_date}')
        SETTINGS mutations_sync = 1
        """

        ph.execute(delete_query, connection=connection_test)
        
        integer_columns = ['likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received']

        user_activity_metrics[integer_columns] = (user_activity_metrics[integer_columns].astype('int64'))

        ph.to_clickhouse(
            df=user_activity_metrics,
            table=table_name,
            index=False,
            connection=connection_test,
        )
        
    feed_tab = extract_feed(event_date='{{ macros.ds_add(ds, -1) }}')

    message_tab = extract_messages(event_date='{{ macros.ds_add(ds, -1) }}')
    
    merged_df = merge_user_activity(feed_tab, message_tab)
    
    user_activity_by_gender = calculate_metrics_by_gender(merged_df)
    user_activity_by_age = calculate_metrics_by_age(merged_df)
    user_activity_by_os = calculate_metrics_by_os(merged_df)
    
    user_activity_metrics = combine_dimension_metrics(user_activity_by_gender, user_activity_by_age, user_activity_by_os)
    
    load(user_activity_metrics, event_date='{{ macros.ds_add(ds, -1) }}')
        
dag_user_activity_egorova = dag_user_activity_egorova()