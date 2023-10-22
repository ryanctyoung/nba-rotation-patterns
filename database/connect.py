import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

MY_DB_DB = os.getenv('db-db')
MY_DB_USER = os.getenv('db-user')
MY_DB_PASS = os.getenv('db-pw')
MY_DB_SERVER = os.getenv('db-server')


def get_sql_session():
    return create_engine('postgresql+psycopg2://{}:{}@{}:5432/{}'.format(MY_DB_USER, MY_DB_PASS, MY_DB_SERVER, MY_DB_DB))


def insert_into_db(df, table_name, conn):
    number = df.to_sql(name=table_name, con=conn, if_exists="append", index=False)
    return number
