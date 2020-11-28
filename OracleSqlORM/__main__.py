# Python to Oracle SQL ORM
from py2sql import *


if __name__ == '__main__':
    print('Python to Oracle SQL ORM Framework')
    orm = Py2SQL()
    db_credentials = DbCredentials('system', 'OraPasswd1', '40.117.92.106/cdb1')
    orm.db_connect(db_credentials)
    print("DB engine: {}".format(orm.db_engine()))
    print("DB name: {}".format(orm.db_name()))
    print("DB size in MB: {}".format(orm.db_size()))
    print("DB tables:...")
    orm.db_disconnect()
