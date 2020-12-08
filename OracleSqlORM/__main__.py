# Python to Oracle SQL ORM
from demo_classes import *
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
        
        print("\n\nSave hierarchy demo")
        print("DB tables: {}".format(orm.db_tables()))
        orm.save_hierarchy(A)
        print("DB tables after saving hierarchy: {}".format(orm.db_tables()))
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))
        orm.delete_hierarchy(A)
        print("DB tables after deleting hierarchy: {}".format(orm.db_tables()))

        print("\n\nSave class and object demo")
        orm.save_class(Test)
        print("DB tables after saving class: {}".format(orm.db_tables()))
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))

        t = Test(3, "test", [1, 3.5, "str"], TestAttr(1.5, {"key": 1, 2.5: "value"}))
        print("TEST table size: " + orm.db_table_size("Test"))
        orm.save_object(t)
        orm.delete_object(t)
        orm.delete_class(Test)
        orm.delete_class(TestAttr)
        print("DB tables after deleting class: {}".format(orm.db_tables()))

    finally:
        orm.db_disconnect()
