import logging
from sqlalchemy import create_engine
import pandas as pd 
from time import sleep
from pathlib import Path

from FUNCTIONS import (get_clean_employee,clean_client, 
                       clean_sales, get_geo)

sleep(10)

#connection postgres. 
# WARNING: If scrypt is used to load online RDS instead of local postgres container, use .env for login information.  

username = 'postgres'
password = 'postgres'
database = 'database'

pg = create_engine(f'postgresql://{username}:{password}@postgres_sample:5432/{database}', echo=True)
pg_connect = pg.connect()

#extract / load data
PATH = Path(__file__).parent/'data'
sales = pd.read_excel(Path(f'{PATH}/random_sales.xlsx', header=6))

employee = pd.read_excel(Path(f'{PATH}/random_employee.xlsx'))
client = pd.read_excel(Path(f'{PATH}/random_client.xlsx'))

#transform
client_clean = clean_client(client)
employee_clean = get_clean_employee(employee)
sales_clean = clean_sales(sales)
clean_geo = get_geo(client_clean)

#load into postgres

client_clean.to_sql('clients',pg_connect,if_exists='replace')
employee_clean.to_sql('employees',pg_connect,if_exists='replace')
sales_clean.to_sql('sales',pg_connect,if_exists='replace')
clean_geo.to_sql('geo',pg_connect,if_exists='replace')
pg_connect.commit()