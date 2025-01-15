from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import random

# Функция для генерации случайного числа и его возведения в квадрат
def generate_and_square_number():
    number = random.randint(1, 100)
    squared = number ** 2
    print(f"Исходное число: {number}, Результат: {squared}")
    return number, squared

# Определение DAG
with DAG('etl_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # BashOperator для генерации случайного числа и его вывода в консоль
    bash_task = BashOperator(
        task_id='generate_random_number',
        bash_command='echo $((RANDOM % 100))',
        dag=dag
    )

    # PythonOperator для генерации случайного числа, его возведения в квадрат и вывода в консоль
    python_task = PythonOperator(
        task_id='generate_and_square_number',
        python_callable=generate_and_square_number,
        dag=dag
    )

    # SimpleHttpOperator для отправки запроса к API погоды
    http_task = SimpleHttpOperator(
        task_id='get_weather',
        http_conn_id='http_default',
        endpoint='weather/Saint-Petersburg',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        dag=dag
    )

    # Установка последовательного порядка выполнения операторов
    bash_task >> python_task >> http_task
