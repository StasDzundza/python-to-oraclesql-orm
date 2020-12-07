# Python to Oracle SQL ORM
from py2sql import *


if __name__ == '__main__':
    print('Python to Oracle SQL ORM Framework')
    orm = Py2SQL()
    db_credentials = DbCredentials('lab3', 'lab3', '40.117.92.106/pdb1')
    orm.db_connect(db_credentials)

    try:
        print("DB engine: {}".format(orm.db_engine()))
        print("DB name: {}".format(orm.db_name()))
        print("DB size in MB: {}".format(orm.db_size()))
        print("DB tables: {}".format(orm.db_tables()))
        print(orm.db_table_size("customers"))
        print(orm.db_table_structure("customers"))

        orm.save_class(Temp)
        print("DB tables: {}".format(orm.db_tables()))
        t = Temp("check_del", 0, [1, "check_del"])
        orm.save_object(t)
        orm.delete_object(t)
        orm.delete_class(Temp)
        print("DB tables: {}".format(orm.db_tables()))

    finally:
        orm.db_disconnect()
