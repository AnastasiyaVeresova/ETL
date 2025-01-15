from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
import requests
import json

# Функция для получения температуры из OpenWeatherMap API
def fetch_weather(**kwargs):
    api_key = 'YOUR_API_KEY'  # Замените на ваш API ключ
    location = 'Moscow'  # Замените на ваше местоположение
    url = f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}&units=metric'
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    print(f'Температура в {location}: {temperature}°C')
    kwargs['ti'].xcom_push(key='temperature', value=temperature)

# Функция для ветвления в зависимости от температуры
def branch_on_temperature(**kwargs):
    temperature = kwargs['ti'].xcom_pull(key='temperature', task_ids='fetch_weather')
    print(f'Полученная температура: {temperature}°C')
    if temperature > 15:
        return 'warm'
    else:
        return 'cold'

# Определение DAG
with DAG('weather_etl_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # Задача для получения температуры
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather,
        provide_context=True,
        dag=dag
    )

    # Задача для ветвления
    branch_task = BranchPythonOperator(
        task_id='branch_on_temperature',
        python_callable=branch_on_temperature,
        provide_context=True,
        dag=dag
    )

    # Задача для вывода "тепло"
    warm_task = BashOperator(
        task_id='warm',
        bash_command='echo "тепло"',
        dag=dag
    )

    # Задача для вывода "холодно"
    cold_task = BashOperator(
        task_id='cold',
        bash_command='echo "холодно"',
        dag=dag
    )

    # Установка последовательного порядка выполнения операторов
    fetch_weather_task >> branch_task >> [warm_task, cold_task]
