import cx_Oracle


class InTemp:
    a = str
    b = int

    def __init__(self, a, b):
        self.a = a
        self.b = b


class Temp:
    user_name = str
    password = int
    test = InTemp

    def __init__(self, user_name, password, test):
        self.user_name = user_name
        self.password = password
        self.test = test


class DbCredentials:
    def __init__(self, user_name, password, host):
        self.user_name = user_name
        self.password = password
        self.host = host


def attrs(obj):
    return dict(i for i in vars(obj).items() if i[0][0] != '_')


class Py2SQL:
    def __init__(self):
        cx_Oracle.init_oracle_client(lib_dir="instantclient_19_9")
        print("client version: {}".format(cx_Oracle.clientversion()))
        self.__connection = None
        self.__db_name = ''

    def db_connect(self, credentials):
        self.__connection = cx_Oracle.connect(credentials.user_name, credentials.password, credentials.host,
                                              encoding='UTF-8')
        if self.__connection is not None:
            print("Connection successful")
            print("Connection version: {}".format(self.__connection.version))
            self.__db_name = credentials.host
        else:
            print("Connection failed")

    def db_disconnect(self):
        if self.__connection is not None:
            self.__connection.close()
            print("Disconnected")
        else:
            print("Not connected")

    def db_engine(self):
        if self.__connection is not None:
            return self.__db_name, self.__connection.version
        else:
            print("Not connected")

    def db_name(self):
        if self.__connection is not None:
            return self.__db_name
        else:
            print("Not connected")

    def db_tables(self):
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
        if self.__connection is not None:
            cursor = self.__connection.cursor()
            cursor.execute("SELECT sum(bytes)/1024/1024 size_in_mb FROM dba_data_files")
            for row in cursor:
                return row[0]
        else:
            print("Not connected")

    def db_table_size(self, table):
        if self.__connection is not None:
            if self.__is_existed(table):
                cursor = self.__connection.cursor()
                cursor.execute("select round(bytes/1024/1024,2) || ' MB' "
                               "from dba_segments "
                               "where segment_name='{}' and segment_type='TABLE'".format(str(table).upper()))
                for row in cursor:
                    return row[0]
            else:
                print("Not exists")
        else:
            print("Not connected")

    def db_table_structure(self, table):
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
        if self.__connection is not None:
            if not self.__is_existed(class_to_save.__name__):
                cursor = self.__connection.cursor()
                for statement in self.__generate_create_table_stmt(class_to_save):
                    cursor.execute(statement)
                self.__connection.commit()
        else:
            print("Not connected")

    def delete_class(self, class_to_delete):
        if self.__connection is not None:
            if self.__is_existed(class_to_delete.__name__):
                cursor = self.__connection.cursor()
                for statement in self.__generate_drop_table_stmt(class_to_delete):
                    cursor.execute(statement)
                self.__connection.commit()
        else:
            print("Not connected")

    def save_object(self, object_to_save):
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

    def __generate_create_table_stmt(self, model):
        statements = []
        model_attrs, collection_attrs, _ = self.__get_attrs_with_types(model)

        columns = ', '.join(['%s %s' % (k, self.__primitive_data_types[v]) for k, v in model_attrs.items()])

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

    def __get_attrs_with_types(self, model):
        model_attrs = attrs(model).items()
        model_attrs = {k: v for k, v in model_attrs}
        collection_attrs = {}
        object_attrs = {}
        for k, v in model_attrs.items():
            type_name = str(v).replace('(', '').replace(')', '') if len(str(v)) != 2 else str(v)
            if type_name in self.__collections_data_types:
                collection_attrs[k] = self.__collections_data_types[type_name]
            if type_name not in self.__collections_data_types and v not in self.__primitive_data_types:
                object_attrs[k] = v
        for k, v in collection_attrs.items():
            del model_attrs[k]
        for k, v in object_attrs.items():
            del model_attrs[k]
            model_attrs[k + "_class_" + v.__name__ + "_id"] = int
            self.save_class(v)

        return model_attrs, collection_attrs, object_attrs
