"""
Module which realize Python to Oracle SQL ORM with the help of Py2SQL class.
"""


import cx_Oracle
import os
from inspect import *


class DbCredentials:
    """Holds db credentials"""
    def __init__(self, user_name, password, host):
        self.user_name = user_name
        self.password = password
        self.host = host


class Py2SQL:
    """Realize Python to Oracle SQL ORM"""
    def __init__(self):
        """
        Invokes when Py2SQL object is created
        Initializes OracleSQL client
        Path to OracleSQL client should be set as ORACLE_CLIENT environment variable.
        """
        oracle_client_dir = os.environ['ORACLE_CLIENT']
        cx_Oracle.init_oracle_client(lib_dir=oracle_client_dir)
        print("client version: {}".format(cx_Oracle.clientversion()))
        self.__connection = None
        self.__db_name = ''

    def db_connect(self, credentials):
        """Initializes and saves connection to remote oracle database"""
        self.__connection = cx_Oracle.connect(credentials.user_name, credentials.password, credentials.host,
                                              encoding='UTF-8')
        if self.__connection is not None:
            print("Connection successful")
            print("Connection version: {}".format(self.__connection.version))
            self.__db_name = credentials.host
        else:
            print("Connection failed")

    def db_disconnect(self):
        """Disconnects from remote database if connected"""
        if self.__connection is not None:
            self.__connection.close()
            print("Disconnected")
        else:
            print("Not connected")

    def db_engine(self):
        """Returns name and version of remote Oracle DBMS if connected"""
        if self.__connection is not None:
            return self.__db_name, self.__connection.version
        else:
            print("Not connected")

    def db_name(self):
        """Returns name of remote Oracle DBMS if connected"""
        if self.__connection is not None:
            return self.__db_name
        else:
            print("Not connected")

    def db_tables(self):
        """Returns list of existing tables of remote Oracle Database if connected"""
        if self.__connection is not None:
            cursor = self.__connection.cursor()
            cursor.execute("SELECT table_name FROM user_tables")
            table_names = []
            for row in cursor:
                table_names.append(row[0])
            cursor.close()
            return table_names
        else:
            print("Not connected")

    def db_size(self):
        """Returns size(in MB) of remote Oracle Database if connected"""
        if self.__connection is not None:
            cursor = self.__connection.cursor()
            cursor.execute("SELECT sum(bytes)/1024/1024 size_in_mb FROM dba_data_files")
            for row in cursor:
                return row[0]
        else:
            print("Not connected")

    def db_table_size(self, table):
        """Returns size of some table(in MB) in Oracle Database if connected"""
        if self.__connection is not None:
            if self.__is_existed(table):
                cursor = self.__connection.cursor()
                cursor.execute("select round(bytes/1024/1024,3) || ' MB' "
                               "from dba_segments "
                               "where segment_name='{}' and segment_type='TABLE'".format(str(table).upper()))
                for row in cursor:
                    return row[0]
            else:
                print("Not exists")
        else:
            print("Not connected")

    def db_table_structure(self, table):
        """ Return ordered table columns and their types """
        attributes = []
        if self.__connection is not None:
            if self.__is_existed(table):
                cursor = self.__connection.cursor()
                cursor.execute('''   select column_id, column_name, data_type
                                       from user_tab_columns
                                      where table_name = '{}'
                                   order by column_id'''.format(str(table).upper()))
                for row in cursor:
                    attributes.append((row[0], row[1], row[2]))
            else:
                print("Not exists")
        else:
            print("Not connected")
        return attributes

    def __is_existed(self, table):
        """ Check if table already exists in database """
        if self.__connection is not None:
            cursor = self.__connection.cursor()
            cursor.execute("select count(table_name) "
                           "from user_tables "
                           "where table_name = '{}'".format(str(table).upper()))
            for row in cursor:
                if int(row[0]) == 0:
                    return False
                else:
                    return True
        else:
            print("Not connected")
            return False

    __primitive_data_types = {str: 'VARCHAR2(4000)', int: 'NUMBER', float: 'FLOAT'}
    __collections_data_types = {'[]': 'LIST', '()': 'TUPLE', 'frozenset': 'FROZENSET',
                                'set': 'SET', '{}': 'DICT'}

    def save_class(self, class_to_save):
        """ Save object representation in database """
        if self.__connection is not None:
            if not self.__is_existed(class_to_save.__name__):
                cursor = self.__connection.cursor()
                for statement in self.__generate_create_table_stmt(class_to_save):
                    cursor.execute(statement)
                self.__connection.commit()
        else:
            print("Not connected")

    def delete_class(self, class_to_delete):
        """ Delete class representation in database """
        if self.__connection is not None:
            if self.__is_existed(class_to_delete.__name__):
                cursor = self.__connection.cursor()
                for statement in self.__generate_drop_table_stmt(class_to_delete):
                    cursor.execute(statement)
                self.__connection.commit()
        else:
            print("Not connected")

    def save_object(self, object_to_save):
        """ Save object representation in database """
        if self.__connection is not None:
            if not self.__is_existed(object_to_save.__class__.__name__):
                self.save_class(object_to_save.__class__)
            cursor = self.__connection.cursor()
            id_var = cursor.var(cx_Oracle.NUMBER)
            statements = self.__generate_insert_stmt(object_to_save)
            insert_params = {}
            for i in range(len(statements)):
                if i == 0:
                    cursor.execute(statements[i], id=id_var)
                    insert_params['id'] = int(id_var.getvalue()[0])
                else:
                    cursor.execute(statements[i].format(**insert_params))
            self.__connection.commit()
            setattr(object_to_save, "db_id", insert_params['id'])
            return insert_params['id']
        else:
            print("Not connected")
            return 0

    def delete_object(self, object_to_delete):
        """ Deletes object representation in database"""
        if self.__connection is not None:
            if self.__is_existed(object_to_delete.__class__.__name__) and getattr(object_to_delete, 'db_id',
                                                                                  None) is not None:
                cursor = self.__connection.cursor()
                for statement in self.__generate_delete_stmt(object_to_delete):
                    cursor.execute(statement)
                self.__connection.commit()
        else:
            print("Not connected")

    def save_hierarchy(self, root_class):
        """
        Save class hierarchy in remote Oracle Database if connected
        It creates table for root class and tables form his children in database
        """
        assert (isclass(root_class))
        if self.__connection is not None:
            children = self.__get_unique_subclasses(root_class)
            for child in children:
                if child and self.__is_existed(child.__name__) is False:
                    self.save_class(child)
        else:
            print("Not connected")

    def delete_hierarchy(self, root_class):
        """
        Remove class hierarchy from remote Oracle Database if connected
        It removes root class table and his children tables
        """
        assert (isclass(root_class))
        if self.__connection is not None:
            children = self.__get_unique_subclasses(root_class)
            cursor = self.__connection.cursor()
            for child in children:
                if child and self.__is_existed(child.__name__):
                    for statement in self.__generate_drop_table_stmt(child):
                        cursor.execute(statement)
            self.__connection.commit()
        else:
            print("Not connected")

    @staticmethod
    def __get_unique_subclasses(root_class):
        """Returns list of unique subclasses of some class, including root class"""
        subclasses = list()
        subclasses.append(root_class)
        class_stack = [root_class]
        while class_stack:
            parent = class_stack.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.append(child)
                    class_stack.append(child)
        return subclasses

    def __generate_create_table_stmt(self, model):
        """ Returns create table statements for input class """
        statements = []
        model_attrs, collection_attrs, _ = self.__get_attrs_with_types(model)
        columns = ['%s %s' % (k, self.__primitive_data_types[v]) for k, v in model_attrs.items()]
        params = {'table_name': str(model.__name__)}
        if len(columns) != 0:
            columns = ', '.join(columns)

            sql = 'CREATE TABLE {table_name} ( ' \
                  'id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, ' \
                  '{columns})'
            params = {'table_name': str(model.__name__), 'columns': str(columns)}
            statements.append(sql.format(**params))

        for k, v in collection_attrs.items():
            params['attr_name'] = k
            params['type'] = v
            if v != 'DICT':
                statements.append('CREATE TABLE {table_name}_{attr_name}_{type} ( '
                                  'object_id INTEGER NOT NULL, '
                                  'value VARCHAR2(4000), '
                                  'value_type VARCHAR2(200))'.format(**params))
            else:
                statements.append('CREATE TABLE {table_name}_{attr_name}_{type} ( '
                                  'object_id INTEGER NOT NULL, '
                                  'key VARCHAR2(4000), '
                                  'key_type VARCHAR2(200), '
                                  'value VARCHAR2(4000), '
                                  'value_type VARCHAR2(200))'.format(**params))

        return statements

    def __generate_drop_table_stmt(self, model):
        """ Returns drop table statements for input class """
        statements = []
        model_attrs, collection_attrs, _ = self.__get_attrs_with_types(model)

        sql = 'DROP TABLE {table_name}'
        params = {'table_name': str(model.__name__)}
        statements.append(sql.format(**params))

        for k, v in collection_attrs.items():
            params['attr_name'] = k
            params['type'] = v
            statements.append('DROP TABLE {table_name}_{attr_name}_{type}'.format(**params))

        return statements

    def __generate_insert_stmt(self, object_to_save):
        """ Returns insert statements for input object """
        model = object_to_save.__class__
        statements = []
        model_attrs, collection_attrs, object_attrs = self.__get_attrs_with_types(model)

        column_defs = []
        column_values = []
        for k, v in model_attrs.items():
            column_defs.append(k)
            if v == str:
                column_values.append("'" + getattr(object_to_save, k) + "'")
            else:
                if k.endswith("_id") and k.find("_class_") != -1 and k[:k.find("_class_")] in object_attrs.keys():
                    column_values.append(str(self.save_object(getattr(object_to_save, k[:k.find("_class_")]))))
                else:
                    column_values.append(str(getattr(object_to_save, k)))

        sql = 'INSERT INTO {table_name} ' \
              '({columns_defs}) ' \
              'VALUES ' \
              '({columns_values})' \
              ' returning Id into :id'

        params = {'table_name': str(model.__name__), 'columns_defs': str(', '.join(column_defs)),
                  'columns_values': str(', '.join(column_values))}

        statements.append(sql.format(**params))

        for k, v in collection_attrs.items():
            params['id'] = '{id}'
            params['attr_name'] = k
            params['type'] = v
            attr = getattr(object_to_save, k)
            if attr is not None:
                if v != 'DICT':
                    for item in attr:
                        params['column_values'] = (str(item) if item.__class__.__name__ != 'str' else "'" + str(
                            item) + "'") + ", '" + item.__class__.__name__ + "'"
                        statements.append('INSERT INTO {table_name}_{attr_name}_{type} '
                                          '(OBJECT_ID, VALUE, VALUE_TYPE) '
                                          'VALUES '
                                          '({id}, {column_values})'.format(**params))
                else:
                    for attr_k, attr_v in attr.items():
                        params['column_values'] = (str(attr_k) if attr_k.__class__.__name__ != 'str' else "'" + str(
                            attr_k) + "'") + ", '" + attr_k.__class__.__name__ + "', " + (
                                                      str(attr_v) if attr_v.__class__.__name__ != 'str' else "'" + str(
                                                          attr_v) + "'") + ", '" + attr_v.__class__.__name__ + "'"
                        statements.append('INSERT INTO {table_name}_{attr_name}_{type} '
                                          '(OBJECT_ID, KEY, KEY_TYPE, VALUE, VALUE_TYPE) '
                                          'VALUES '
                                          '({id}, {column_values})'.format(**params))

        return statements

    def __generate_delete_stmt(self, object_to_save):
        """ Returns delete statements for input object """
        model = object_to_save.__class__
        object_id = object_to_save.db_id
        statements = []
        model_attrs, collection_attrs, object_attrs = self.__get_attrs_with_types(model)

        sql = 'DELETE FROM {table_name} WHERE ID = {id}'
        params = {'table_name': str(model.__name__), 'id': object_id}

        statements.append(sql.format(**params))

        for k, v in collection_attrs.items():
            params['attr_name'] = k
            params['type'] = v
            statements.append('DELETE FROM {table_name}_{attr_name}_{type} WHERE OBJECT_ID = {id}'.format(**params))

        return statements

    def __get_attrs_with_types(self, model):
        """ Returns class attributes and their types divided into primitive, collection and object attributes """
        model_attrs = self.attrs(model).items()
        model_attrs = {k: v for k, v in model_attrs}
        collection_attrs = {}
        object_attrs = {}
        methods_to_del = []
        for k, v in model_attrs.items():
            type_name = str(v).replace('(', '').replace(')', '') if len(str(v)) != 2 else str(v)
            if type_name in self.__collections_data_types:
                collection_attrs[k] = self.__collections_data_types[type_name]
            if type_name.find("function") != -1 or type_name.find("property") != -1 or type_name.find("method") != -1:
                methods_to_del.append(k)
                continue
            if type_name not in self.__collections_data_types and v not in self.__primitive_data_types:
                object_attrs[k] = v
        for k in methods_to_del:
            del model_attrs[k]
        for k, v in collection_attrs.items():
            del model_attrs[k]
        for k, v in object_attrs.items():
            del model_attrs[k]
            model_attrs[k + "_class_" + v.__name__ + "_id"] = int
            self.save_class(v)

        return model_attrs, collection_attrs, object_attrs

    @staticmethod
    def attrs(model):
        """ Returns dict of class attributes and their types"""
        return dict(i for i in vars(model).items() if i[0][0] != '_')


class Model:

    __orm = Py2SQL()
    __db_credentials = None

    def delete(self):
        """ Delete object representation in database """
        self.__orm.db_connect(self.__db_credentials)
        try:
            self.__orm.delete_object(self)
        finally:
            self.__orm.db_disconnect()

    def save(self):
        """ Save object representation in database """
        self.__orm.db_connect(self.__db_credentials)
        try:
            self.__orm.save_object(self)
        finally:
            self.__orm.db_disconnect()

    @classmethod
    def save_class(cls, db_credentials):
        """ Save class representation in database """
        cls.__orm.db_connect(db_credentials)
        try:
            cls.__orm.save_class(cls)
        finally:
            cls.__orm.db_disconnect()

    @classmethod
    def delete_class(cls, db_credentials):
        """ Delete object representation in database """
        cls.__orm.db_connect(db_credentials)
        try:
            cls.__orm.delete_class(cls)
        finally:
            cls.__orm.db_disconnect()

    def set_db_credentials(self, db_credentials):
        self.__db_credentials = db_credentials
