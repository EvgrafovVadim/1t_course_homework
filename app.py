import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def process_data(engine):
    conn = engine.connect()

    data = pd.read_sql('SELECT age FROM test_table WHERE LENGTH(fistname)<6;', conn)

    min = data.min()
    max = data.max()

    return min, max

if __name__ == "__main__":
    db_user = 'postgres'
    db_password = 'password'
    db_host = 'db'
    db_port = '5432'
    db_name = 'postgres'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    result = process_data(engine)

    print("миниальный и максимальный возраст равны--")
    print(result)