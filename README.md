# python-oraclesql-orm
Python to Oracle SQL Object Relationship Mapping(ORM)

## Authors
- Alexandra Kmet
- Stanislav Dzundza

## Install
###### Preinstallation:
1. Download [Oracle Instant Client](https://www.oracle.com/uk/database/technologies/instant-client/downloads.html) for your OS
2. Unzip it
3. Create ORACLE_CLIENT environment variable with unzipped directory path
4. Reload your machine

###### Installation
Project can be downloaded or installed using PyPI
To install from PyPI:
- ensure you have already installed Python 3.7+ and pip on machine
- open terminal and run command `pip install py-2-oracle-sql-orm`

## Database
We provide database deployed on Azure
```
User: lab3
Password: lab3
Host: 40.117.92.106/pdb1
```
## Usage

Rules:
1. Types of attribule must be specified

 ```py
  class Test:
    int_attr = int
    str_attr = str
    list_attr = list()
    object_attr = TestAttr
    
    def __init__(self, int_attr, str_attr, list_attr, object_attr):
        self.int_attr = int_attr
        self.str_attr = str_attr
        self.list_attr = list_attr
        self.object_attr = object_attr

   ```
   *Supported types:* `int`*,* `float`*,* `str`*,* `list`*,* `tuple`*,* `dict`*,* `set`*,* `frozenset`*,* `object`

2. Create instanse of `DbCredentials` and set your database credentials

    ```py
    >>> db_credentials = DbCredentials('lab3', 'lab3', '40.117.92.106/pdb1')
    ```
3. Create instanse of `Py2SQL` and connect to database
    ```py
    >>> orm = Py2SQL()
    >>> orm.db_connect(db_credentials)
    ```
4. Use provided methods
5. Don't forget to disconnect
 ```py
    >>> orm.db_disconnect()
 ```
#### Example
  For better understanding it is highly recommended to start from examples included in project
  
  Classes for demonstration are located in [demo_classes.py](OracleSqlORM/demo_classes.py)
  
  Run [main.py](OracleSqlORM/__main__.py) to start demonstration
  
    
