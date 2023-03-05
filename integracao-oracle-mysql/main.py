import configparser

import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import warnings


config = configparser.ConfigParser()
config.read('config.ini')

def database_connect():
    """starts oracle client and returns connection object"""
    cx_Oracle.init_oracle_client(lib_dir=r'D:\PycharmProjects\integracao-oracle-mysql\instantclient_19_9')

    user = config['ORACLE']['user']
    password = config['ORACLE']['password']
    ip = config['ORACLE']['ip']
    sid = config['ORACLE']['sid']

    connection = cx_Oracle.connect(
        user,
        password,
        f'{ip}/{sid}',
        encoding="UTF-8")

    return connection

#warnings.filterwarnings('ignore')

# list out all 43 tables:
table_list = [
    "EMPLOYEES", "DEPARTMENTS"
]

# Set Oralce Connection
#dsn_tns = cx_Oracle.makedsn('127.0.0.1', '8080', service_name='xe')
oracle_connection = database_connect()

# Open Oracle cursor
cursor = oracle_connection.cursor()

# set mysql connection with foreign key checks
#mysql_engine = create_engine("mysql+pymysql://root:toot@target.example.com:3306/target")
#mysql_engine.execute("SET FOREIGN_KEY_CHECKS=0")

# loop thru tables:
for table in table_list:

    # select from oracle
    sql = "SELECT * FROM " + table

    # read into pandas df
    data=pd.read_sql(sql, oracle_connection)

    print(data)

    # insert into mysql
'''    mysql_engine.execute("TRUNCATE TABLE "+table)
    data.to_sql(table, con=mysql_engine, if_exists='append', index=False, chunksize=10000)
    print("{}: sucessfully inserted {} rows.".format(table, data.shape[0]))'''

# update foreign key checks
#mysql_engine.execute("SET FOREIGN_KEY_CHECKS=1")

#close connection
oracle_connection.close()
#mysql_engine.dispose()