from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

# Функция для загрузки данных из CSV файлов
def load_data(**kwargs):
    booking_df = pd.read_csv('/path/to/csv/booking.csv')
    client_df = pd.read_csv('/path/to/csv/client.csv')
    hotel_df = pd.read_csv('/path/to/csv/hotel.csv')
    kwargs['ti'].xcom_push(key='booking_df', value=booking_df)
    kwargs['ti'].xcom_push(key='client_df', value=client_df)
    kwargs['ti'].xcom_push(key='hotel_df', value=hotel_df)

# Функция для трансформации данных
def transform_data(**kwargs):
    ti = kwargs['ti']
    booking_df = ti.xcom_pull(key='booking_df', task_ids='load_data')
    client_df = ti.xcom_pull(key='client_df', task_ids='load_data')
    hotel_df = ti.xcom_pull(key='hotel_df', task_ids='load_data')

    # Объединение таблиц
    merged_df = pd.merge(booking_df, client_df, on='client_id')
    merged_df = pd.merge(merged_df, hotel_df, on='hotel_id')

    # Приведение дат к одному виду
    merged_df['booking_date'] = pd.to_datetime(merged_df['booking_date'], errors='coerce')

    # Удаление невалидных колонок
    merged_df.dropna(subset=['booking_cost', 'currency'], inplace=True)

    # Приведение всех валют к EUR
    currency_conversion = {
        'GBP': 1.15,  # Примерный курс GBP к EUR
        'EUR': 1.0
    }
    merged_df['booking_cost_eur'] = merged_df.apply(lambda row: row['booking_cost'] * currency_conversion[row['currency']], axis=1)

    # Удаление ненужных колонок
    merged_df.drop(columns=['currency', 'booking_cost'], inplace=True)

    kwargs['ti'].xcom_push(key='transformed_df', value=merged_df)

# Функция для загрузки данных в базу данных
def load_to_database(**kwargs):
    ti = kwargs['ti']
    transformed_df = ti.xcom_pull(key='transformed_df', task_ids='transform_data')

    # Создание соединения с базой данных
    engine = create_engine('mysql+pymysql://root:123567@localhost:3306/spark')
    


    # Загрузка данных в базу данных
    transformed_df.to_sql('hotel_bookings', con=engine, if_exists='replace', index=False)

# Определение DAG
with DAG('etl_hotel_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # Задача для загрузки данных
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        dag=dag
    )

    # Задача для трансформации данных
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        dag=dag
    )

    # Задача для загрузки данных в базу данных
    load_to_database_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        provide_context=True,
        dag=dag
    )

    # Установка последовательного порядка выполнения операторов
    load_data_task >> transform_data_task >> load_to_database_task
