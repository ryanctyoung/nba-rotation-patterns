import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy as sa

load_dotenv()

MY_DB_DB = os.getenv('db-db')
MY_DB_USER = os.getenv('db-user')
MY_DB_PASS = os.getenv('db-pw')
MY_DB_SERVER = os.getenv('db-server')

test_suffix = '_test'


def get_sql_session():
    return sa.create_engine('postgresql+psycopg2://{}:{}@{}:5432/{}'.format(MY_DB_USER, MY_DB_PASS, MY_DB_SERVER, MY_DB_DB))


def insert_into_db(df, table_name, conn):
    number = df.to_sql(name=table_name, con=conn, if_exists="append", index=False)
    return number


def df_upsert(df, table_name, conn, schema=None, match_columns=None):
    """
    Perform an "upsert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
    temporary table, and then executes the INSERT.
    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    conn : sqlalchemy.engine.Connection
        The connection to SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    match_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    """
    table_spec = ""
    if schema:
        table_spec += '"' + schema.replace('"', '""') + '".'
    table_spec += '"' + table_name.replace('"', '""') + '"'

    df_columns = list(df.columns)
    if not match_columns:
        insp = sa.inspect(conn)
        match_columns = insp.get_pk_constraint(table_name, schema=schema)[
            "constrained_columns"
        ]
    columns_to_update = [col for col in df_columns if col not in match_columns]
    insert_col_list = ", ".join([f'"{col_name}"' for col_name in df_columns])
    stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
    stmt += f"SELECT {insert_col_list} FROM temp_table\n"
    match_col_list = ", ".join([f'"{col}"' for col in match_columns])
    stmt += f"ON CONFLICT ({match_col_list}) DO UPDATE SET\n"
    stmt += ", ".join(
        [f'"{col}" = EXCLUDED."{col}"' for col in columns_to_update]
    )

    conn.exec_driver_sql("DROP TABLE IF EXISTS temp_table")
    conn.exec_driver_sql(
        f"CREATE TEMPORARY TABLE temp_table AS SELECT * FROM {table_spec} WHERE false"
    )
    df.to_sql("temp_table", conn, if_exists="append", index=False)
    conn.exec_driver_sql(stmt)
