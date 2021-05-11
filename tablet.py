from typing import Union
import pyodbc
import sqlite3
from sqlite3 import Error
import queue
from datetime import *
import pymysql
import rds_config as rds

store = 19

def get_tickets():
    server = f'10.10.{store}.123' 
    database = 'LHSiteDB' 
    username = 'parkerbi' 
    password = 'P@rkersbi18!' 
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cur = conn.cursor()

    query = "Select top 15 f_speed_of_service.business_date AS Date, \
                    f_speed_of_service.tran_start_terminal_id AS CSSTerminal, \
                    f_speed_of_service.tran_sequence_number AS TranSeqNumber, \
                    CAST(f_speed_of_service.tran_start_time AS varchar) as TranSubmit, \
                    f_speed_of_service.tran_store_time as TranOrderStoreTime, \
                    f_speed_of_service.tran_recall_time AS TranRecallTime, \
                    f_speed_of_service.tran_recall_terminal_id as TranRecallTerminal, \
                    f_speed_of_service.tran_bump_time as TranBumpTime, \
                    f_speed_of_service.tran_pay_time as TranPayTime, \
                    f_speed_of_service.tran_pay_terminal_id as TranPayTerminal, \
                    f_speed_of_service.last_modified_timestamp as LastModified, \
                    CASE \
                        WHEN f_speed_of_service.tran_pay_time is null and f_speed_of_service.tran_bump_time is null then 'Awaiting Payment' \
                        when f_speed_of_service.tran_pay_time is null and f_speed_of_service.tran_bump_time is not null then 'Awaiting Payment' \
                        when f_speed_of_service.tran_pay_time is not null and f_speed_of_service.tran_bump_time is null then 'In Progress'   \
                        else 'Ready for Pick Up' \
                        end as Status \
                    From f_speed_of_service \
                    Where f_speed_of_service.business_date = Cast(DateAdd(DD, 0, GetDate()) As DATE) \
                    and f_speed_of_service.tran_store_time is not null \
                    order by f_speed_of_service.tran_start_time desc;"

    cur.execute(query)
    return cur.fetchall()


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


def get_orders():
    # Get current tickets
    check_time = datetime.now() - timedelta(minutes=20)

    my_sql_select = f"Select tran_number, status, tran_submit_string, tran_key from orders where store = {store} and submit_time >= '{check_time}' and (status != 'Complete' or status is NULL) order by tran_number;"

    orig_orders = my_sql(my_sql_select)

    for orig_item in orig_orders:
        tran_number = orig_item.get('tran_number')
        orig_status = orig_item.get('tran_number')
        time = orig_item.get('tran_submit_string')

    ## Live DB Statuses
    r = get_tickets()

    for row in r:
        date = row[0]
        tran_terminal = row[1]
        tran_seq_number = str(row[2])
        tran_submit_string = str(row[3])
        submit_time = row[4]
        recall_time = row[5]
        bump_time = row[7]
        pay_time = row[8]
        pay_terminal = row[9]
        last_modified = row[10]
        status = row[11]

        tran_key = f"{store}-{date}-{tran_seq_number}"
                

        my_sql_insert = f"INSERT into orders (tran_key, tran_number, store, date, tran_terminal, submit_time, tran_submit_string, recall_time, bump_time, pay_time, pay_terminal, last_modified, status) \
            values ('{tran_key}', '{tran_seq_number}', '{store}', '{date}', '{tran_terminal}', '{submit_time}', '{tran_submit_string}', '{recall_time}', '{bump_time}', '{pay_time}', '{pay_terminal}', '{last_modified}', '{status}')\
                on DUPLICATE KEY UPDATE  \
                    recall_time = '{recall_time}', \
                    bump_time = '{bump_time}', \
                    pay_time = '{pay_time}', \
                    pay_terminal = '{pay_terminal}', \
                    last_modified = '{last_modified}', \
                    status = CASE status when 'Complete' then status else '{status}' END;"

        my_sql(my_sql_insert)

    old_orders = my_sql(f"Select tran_number from orders where submit_time < '{check_time}';")

    row_dict = dict()
    for old_item in old_orders:
        try:
            item_key = old_item.get('tran_number')
            row_dict[item_key]['frame'].forget_widget()
        except Exception as e:
            #print(e)
            continue

    orders = my_sql(my_sql_select)

    i=0
    for item in orders:
        tran_number = item.get('tran_number')
        status = item.get('status')
        time = item.get('tran_submit_string')
        tran_key = item.get('tran_key')


get_orders()



