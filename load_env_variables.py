import os
from dotenv import load_dotenv

# Need to create an .env file following .env.example file
load_dotenv()

mysql_server = os.getenv('MYSQL_SERVER')
mysql_db = os.getenv('MYSQL_DATABASE')
mysql_db_username = os.getenv('MYSQL_DB_USERNAME')
mysql_db_password = os.getenv('MYSQL_DB_PASSWORD')

mssql_server = os.getenv('MSSQL_SERVER')
mssql_db = os.getenv('MSSQL_DATABASE')
