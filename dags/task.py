from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from pytz import timezone
import os 


local_tz = timezone('Asia/Ho_Chi_Minh')


# Định nghĩa các hàm xử lý dữ liệu
def process_data():
    print('process data')
    
def save_data():
    print('save data')
    print()


# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 7, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5000),
}

dag = DAG(
    dag_id='my_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='* * * * *'
)

# Định nghĩa các Task
task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Task 1"',
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=process_data,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=save_data,
    dag=dag,
)

# Thiết lập phụ thuộc giữa các Task
task1 >> task2 >> task3

if __name__ == "__main__":
    dag.cli()
