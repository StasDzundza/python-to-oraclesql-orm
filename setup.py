import pathlib
from setuptools import *

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE/"README.md").read_text()

setup(
      name='py_2_oracle_sql_orm',
      version='1.0.1',
      description='Python-Oracle SQL ORM',
      long_description=README,
      long_description_content_type="text/markdown",
      packages=['OracleSqlORM'],
	  install_requires=['cx_Oracle'],
      author="Alexandra Kmet, Stanislav Dzundza",
)