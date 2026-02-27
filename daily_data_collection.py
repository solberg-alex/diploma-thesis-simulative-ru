import requests as rq
import psycopg2 as p2
from datetime import datetime, timedelta
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO

api_url = 'http://final-project.simulative.ru/data'
today = datetime.today() - timedelta(days=1)
str_today = f'{today.year}-{today.month:02d}-{today.day:02d}'
params = {'date': str_today}
response = rq.get(api_url, params=params)

result = load_dotenv()
if not result:
    raise EnvironmentError('Не удалось загрузить файл .env или переменные окружения')

db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')
db_port = os.getenv('DB_PORT')

conn = p2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
cursor = conn.cursor()

upload_df = pd.DataFrame(response.json())

def upload_to_db(df, table_name):
    output = StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    columns = (
        'client_id', 'gender', 'purchase_datetime', 
        'purchase_time_as_seconds_from_midnight', 'product_id', 
        'quantity', 'price_per_item', 'discount_per_item', 'total_price'
    )
    cursor.copy_from(output, table_name, sep='\t', columns=columns)
    conn.commit()

upload_to_db(upload_df, 'sales')

conn.close()