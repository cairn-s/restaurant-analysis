"""
Milestone 3

Nama  : Fauzan Abdurrochman Anwar
Batch : RMT-059

This program contain an automized process to make a sql table, data cleaning, send data to elasticsearch for analysis
Dataset used is about cafe transactions on different outlets 
"""

from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers

from datetime import datetime, timedelta

import pandas as pd

db_engine = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
engine = create_engine(db_engine)
db_schema = "public"
db_table = "table_m3"

def load_data():
    """
    Load raw csv file into postgres.

    Reads raw csv file and load into SQL with table name 'table_m3'
    Replacing the table if it already exists.
    """
    df = pd.read_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_raw.csv', delimiter=',')
    df.to_sql(db_table, engine, schema=db_schema, if_exists='append', index=False)


def remove_dupes():
    """
    Remove duplicate rows from raw data

    Read raw data and drop any duplicated rows then save it into a CSV file
    """
    df = pd.read_sql(
        'SELECT * FROM table_m3', con=engine)
    
    df.drop_duplicates(inplace=True)
    df.to_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv', index=False)

def column_norm():
    """
    Normalize columns name into lowercase and replace whitespaces with underscore

    Read the CSV file then normalizes the columns, overwrite the previous CSV file
    """
    df = pd.read_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv', delimiter=',')
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.to_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv', index=False)

def miss_val():
    """
    Fill missing values for both numerical & categorical
    
    Read the CSV file and impute categorical missing values with mode & numerical with median
    writes the final cleaned data
    """
    df = pd.read_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv', delimiter=',')
    cat_col = df.select_dtypes(include=['object'])
    num_col = df.select_dtypes(exclude=['object'])
    df[cat_col.columns] = df[cat_col.columns].fillna(df[cat_col.columns].mode().iloc[0]) # fill categorical value with modus
    df[num_col.columns] = df[num_col.columns].fillna(df[num_col.columns].median()) # fill numerical value with median
    df.to_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv', index=False)

def to_es():
    """
    Connect with elastic search and send the cleaned csv file to it
    """
    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/data/P2M3_fauzan_abdurrochman_data_clean.csv')
    actions = [
    {
        "_index": "data_m3",
        "_source": row.to_dict(),
    }
    for _, row in df.iterrows()
]
    helpers.bulk(es, actions, chunk_size=1000)

default_args= {
    'owner': 'fauzan',
    'start_date': datetime(2024, 11, 2, 9, 0, 0) - timedelta(hours=7)
}

with DAG(
    "H8_P2M3",
    description='Milestone',
    schedule_interval='10-30/10 9 * * 6',
    default_args=default_args, 
    catchup=False,
    max_active_runs=1) as dag:

#===========task1===============
    make_table=PostgresOperator(
        task_id = 'make_table',
        postgres_conn_id = 'postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS table_m3 (
           "Outlet" VARCHAR(255), 
           "Date" DATE,
           "Time" TIME(0),
           "NetSales" FLOAT,
           "Tax" FLOAT,
           "TotalAmount" INT,
           "ReceiptNumber" VARCHAR(255),
           "Items" VARCHAR(255),
           "TotalItem" INT,
           "PaymentMethod" VARCHAR(255),
           "UseLoyaltyCard" INT
            );
        '''
    )
#===========task2===============
    load_task=PythonOperator(
        task_id='load_data',
        python_callable=load_data)

    
#==========task3================
    with TaskGroup('data_cleaning') as data_cleaning:
        # 3.1
        dupes_task=PythonOperator(
            task_id="remove_dupes",
            python_callable=remove_dupes
        )

        # 3.2
        column_normalization=PythonOperator(
            task_id="normalize_column",
            python_callable=column_norm
        )

        #3.3
        miss_value=PythonOperator(
            task_id="handling_miss_val",
            python_callable=miss_val
        )

#===========task 4================
    pass_es=PythonOperator(
        task_id='post_elastic',
        python_callable=to_es,
        retries=3,
        retry_delay=timedelta(seconds=30)
    )



make_table >> load_task >> dupes_task >> column_normalization >> miss_value >> pass_es