from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return 'Привет, Анастасия!'

dag = DAG('my_first_dag', description='Hello DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
