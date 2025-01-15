"""Этот DAG (Directed Acyclic Graph) в Apache Airflow выполняет процесс обучения трех моделей, выбора лучшей модели на основе точности
и выполнения соответствующих действий в зависимости от точности модели.
"""

#Определение DAG и задач
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

#Функции для обучения моделей и выбора лучшей модели

"""_choosing_best_model(ti):
Эта функция извлекает точности моделей из XCom (механизм обмена данными между задачами в Airflow) и выбирает лучшую модель.
Если максимальная точность больше 8, возвращается 'accurate', иначе 'inaccurate'.
"""
def _choosing_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
    'training_model_A',
    'training_model_B',
    'training_model_C'
    ])
    if max(accuracies) > 8:
        return 'accurate'
    return 'inaccurate'

"""
_training_model(model):
Эта функция имитирует процесс обучения модели и возвращает случайное значение точности от 1 до 10

DAG("my_dag", ...): Определяет DAG с именем my_dag, который начинается с 1 января 2021 года и выполняется ежедневно.
training_model_tasks: Создает три задачи для обучения моделей A, B и C. Каждая задача вызывает функцию _training_model и передает идентификатор модели в качестве аргумента.
choosing_best_model: Создает задачу BranchPythonOperator, которая вызывает функцию _choosing_best_model для выбора лучшей модели.
accurate и inaccurate: Создает две задачи BashOperator, которые выполняют команды 'accurate' и 'inaccurate' соответственно.

training_model_tasks >> choosing_best_model: Устанавливает зависимость, что задача choosing_best_model будет выполнена после завершения всех задач обучения моделей.
choosing_best_model >> [accurate, inaccurate]: Устанавливает зависимость, что задачи accurate и inaccurate будут выполнены после завершения задачи choosing_best_model.
Однако, только одна из этих задач будет выполнена в зависимости от результата функции _choosing_best_model
"""
def _training_model(model):
    return randint(1, 10)
with DAG("my_dag", start_date=datetime(2021, 1 ,1), schedule_interval='@daily', catchup=False) as dag:
    training_model_tasks = [
        PythonOperator(
        task_id=f"training_model_{model_id}",
        python_callable=_training_model,
        op_kwargs={"model": model_id}
        ) for model_id in ['A', 'B', 'C']
    ]
    choosing_best_model = BranchPythonOperator(
        task_id="choosing_best_model",
        python_callable=_choosing_best_model
        )
    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
        )
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command=" echo 'inaccurate'"
        )
training_model_tasks >> choosing_best_model >> [accurate, inaccurate]

""" 
Процесс выполнения DAG
Обучение моделей:

Три задачи training_model_A, training_model_B и training_model_C выполняются параллельно. Каждая задача возвращает случайное значение точности модели.
Выбор лучшей модели:

Задача choosing_best_model извлекает точности моделей из XCom и выбирает лучшую модель. Если максимальная точность больше 8, возвращается 'accurate', иначе 'inaccurate'.
Выполнение соответствующей задачи:

В зависимости от результата задачи choosing_best_model, выполняется либо задача accurate, либо задача inaccurate.
"""