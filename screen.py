from typing import Union
import pyodbc
import sqlite3
from sqlite3 import Error
from datetime import *
import pymysql
import rds_config as rds

store = 19

row_dict = dict()

def my_sql(query: str, database='css_order_screens', *args) -> Union[dict, None]:
    """Query MySQL DB using 

    Args:
        query (str): Query to execute in DB

    Returns:
        tuple: Query results in tuple format
        None: Returns None for UPDATE/INSERT

    Usage\n
    >>> x = self.my_sql('SELECT * from table') -> dict
    >>> x = self.my_sql('INSERT INTO table', database='css_order_screen') -> None
    """

    conn = pymysql.connect(host=rds.host,
                            user=rds.user,
                            password=rds.password,
                            database=database,
                            cursorclass=pymysql.cursors.DictCursor
                            )

    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()
        response = cur.fetchall()
    conn.close()
    return response

def get_mysql_tickets():
    check_time = datetime.now() - timedelta(minutes=20)
    select_query = f"Select tran_number, status, submit_time, tran_key from orders where store = {store} and submit_time >= '{check_time}' and (status != 'Complete' or status is NULL) order by tran_number asc;"
    ## Live DB Statuses
    orders = my_sql(select_query)
    old_orders = my_sql(f"Select tran_key from orders where store = {store} and submit_time < '{check_time}';")

    return orders,old_orders

def get_orders():
    # Get current tickets
    
    orders,old_orders = get_mysql_tickets()

get_orders()


