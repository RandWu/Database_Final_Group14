import oracledb
import pandas as pd
import numpy as np
import sys
from typing import Union

if sys.version_info < (3,9):
    # inform user to use new Python
    print('Please run this program with Python 3.9 or newer')
    sys.exit(1)

def connect() -> oracledb.Connection:
    return oracledb.connect(
        user="GROUP14",
        password="hd9qaY5L2t",
        dsn=oracledb.makedsn("140.117.69.60", 1521, service_name='ORCLPDB1')
    )

conn = connect()
cursor = conn.cursor()
# read dataset
data = pd.read_csv(r"ECommerce_consumer_behaviour.csv")
# convert nan to -1, meaning this is first time for this customer
data['days_since_prior_order'] = data.loc[:,['days_since_prior_order']].replace(np.nan, -1)
# remove any rows that contain na or nan
data = data.dropna()

def insert(connection: oracledb.Connection,
           cursor: oracledb.Cursor,
           datafram: pd.DataFrame,
           colums: Union[list, tuple],
           types: Union[list, tuple],
           sql: str
           ) -> bool:
    if len(colums) != len(types):
        print("Length of columns and types mismatch.")
        return False
    
    for _, row in datafram.iterrows():
        values = []
        for col, typ in zip(colums, types):
            try:
                value = typ(row[col])
            except (TypeError, ValueError) as e:
                print(typ)
                print(e)
                print(row)
                return False
            values.append(value)

        try:
            cursor.execute(sql, values)
        except oracledb.Error as e:
            print("Execute error!")
            print(values)
            print(e)
            return False
    try:
        connection.commit()
    except oracledb.Error as e:
        print("commit error!")
        print(e)
        return False
    return True


# insert values to Users table
print("Insert to User ...")
users_id = data['user_id'].unique()
users = pd.DataFrame(users_id, columns=['user_id'])
sql = "INSERT INTO USERS (user_id) VALUES (:1)"
results = insert(conn, cursor, users, ['user_id'], (int,), sql)
if not results:
    sys.exit(1)
print("Insert to User complete!")

cursor = conn.cursor()
# insert values to department2 tables
depts = data.loc[:,['department_id', 'department']].drop_duplicates()
print("Insert to Departments2 ...")
sql = "INSERT INTO DEPARTMENTS2 (DEPARTMENT_ID, DEPARTMENT) VALUES (:1, :2)"
results = insert(conn, cursor, depts,
                 ['department_id', 'department'],
                 (int, str),
                 sql)
if not results:
    conn.close()
    sys.exit()
print("Insert to Departments2 complete!")

cursor = conn.cursor()
# insert values to products table
products = data.loc[:, ['product_id', 'department_id', 'product_name']].drop_duplicates()
print("Insert to Products ...")
sql = "INSERT INTO PRODUCTS (product_id, department_id, product_name) VALUES (:1, :2, :3)"
results = insert(conn, cursor, products,
                 ['product_id', 'department_id', 'product_name'],
                 (int, int, str),
                 sql)
if not results:
    sys.exit(1)
print("Insert to Products complete!")

cursor = conn.cursor()
# insert values to orders table
orders = data.loc[:, ['order_id', 'user_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']].drop_duplicates()
print("Insert to Orders ...")  
sql = "INSERT INTO ORDERS (order_id, user_id, order_dow, order_hour_of_day, days_since_prior_order) VALUES (:1, :2, :3, :4, :5)"
results = insert(conn, cursor, orders, 
                 ['order_id', 'user_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order'],
                 (int, int, int, int, int),
                 sql)
if not results:
    sys.exit(1)
print("Insert to Orders complete!")    

cursor = conn.cursor()
# insert values to Order_Details tables
print("Insert to ORDER_DETAILS ...")
sql = "INSERT INTO Order_Details (order_id, product_id, add_to_cart_order, reordered) VALUES (:1, :2, :3, :4)"
results = insert(conn, cursor, data,
                 ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
                 (int, int, int, int),
                 sql)
if not results:
    sys.exit(1)
print("Insert to Order_Details complete!")

conn.close()