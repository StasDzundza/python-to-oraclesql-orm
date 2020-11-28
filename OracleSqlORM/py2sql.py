import cx_Oracle


class DbCredentials:
    def __init__(self, user_name, password, host):
        self.user_name = user_name
        self.password = password
        self.host = host


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
        if self.__connection.is not None:
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
            cursor.execute("SELECT table_name FROM dba_tables")
            table_names = []
            for row in cursor:
                table_names.append(row[0])
            cursor.close()
        print("Not connected")
