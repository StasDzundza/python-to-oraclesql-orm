# Python to Oracle SQL ORM
from py2sql import *


class Book(Model):
    a = int
    b = str

    def __init__(self, a, bb):
        self.a = a
        self.b = bb


class BookA(Book):
    c = dict


class BookB(Book):
    c = float


class Liss:
    c = float
    book = Book


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
        Book.save_class(db_credentials)
        book = Book(5, "text")
        book.set_db_credentials(db_credentials)
        book.save()
        book.delete()
        Book.delete_class(db_credentials)
        print("DB tables: {}".format(orm.db_tables()))

        '''orm.save_hierarchy(Book)
        print("DB tables after saving hierarchy: {}".format(orm.db_tables()))
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))
        orm.delete_hierarchy(Book)
        print("DB tables after deleting hierarchy: {}".format(orm.db_tables()))

        print("\n\nSave class and object demo")
        orm.save_class(Liss)
        print("DB tables after saving class: {}".format(orm.db_tables()))
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))

        book = Book()
        book.a = 1
        book.b = '2'
        liss = Liss()
        liss.c = 9.8
        liss.book = book

        # print("TEST table size: " + orm.db_table_size("Test"))
        orm.save_object(liss)
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))
        orm.delete_object(liss)
        for table in orm.db_tables():
            print("Table {} structure: ".format(table))
            print(orm.db_table_structure(table))
        orm.delete_class(Liss)
        print("DB tables after deleting class: {}".format(orm.db_tables()))
        orm.delete_class(Liss)
        print("DB tables after deleting class: {}".format(orm.db_tables()))'''

    finally:
        orm.db_disconnect()
