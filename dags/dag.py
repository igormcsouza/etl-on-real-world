from datetime import datetime, timedelta

from airflow import DAG


default_args = {
    'owner': 'airflow',
    'depends_on_past': 'False',
    'start_date': datetime(2021, 04, 21),
    'retries': 0
}

dag = DAG(dag_id='DAG-1', default_args=default_args, catchup=False)