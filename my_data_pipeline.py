from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from tests.unit_test import test_load_data, test_calculate_movie_ratings, test_get_top3_movies_by_user, test_write_data
from dqTest.dq_test import test_movies_data_quality, test_ratings_data_quality

# Set your default_args
default_args = {
    'owner': 'Himanshu',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate your DAG
dag = DAG(
    'my_data_pipeline',
    default_args=default_args,
    description='data pipeline for movies reating app',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Define PythonOperator tasks to run your tests

test_load_data_task = PythonOperator(
    task_id='test_load_data',
    python_callable=test_load_data,  # Your test function
    provide_context=False,
    dag=dag,
)

test_calculate_movie_ratings_task = PythonOperator(
    task_id='test_calculate_movie_ratings',
    python_callable=test_calculate_movie_ratings,  # Your test function
    provide_context=False,
    dag=dag,
)

test_get_top3_movies_by_user_task = PythonOperator(
    task_id='test_get_top3_movies_by_user',
    python_callable=test_get_top3_movies_by_user,  # Your test function
    provide_context=False,
    dag=dag,
)

test_write_data_task = PythonOperator(
    task_id='test_write_data',
    python_callable=test_write_data,  # Your test function
    provide_context=False,
    dag=dag,
)

test_movies_task = PythonOperator(
    task_id='test_movies',
    python_callable=test_movies_data_quality,  # Your test function
    provide_context=False,
    dag=dag,
)

test_ratings_task = PythonOperator(
    task_id='test_ratings',
    python_callable=test_ratings_data_quality,  # Your test function
    provide_context=False,
    dag=dag,
)

# Define BashOperator tasks to run your Spark job
spark_job_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --master <your_spark_master_url> your_spark_job_script.py',
    dag=dag,
)

# Set task dependencies
test_load_data_task >> test_calculate_movie_ratings_task >> test_get_top3_movies_by_user_task >> test_write_data_task >> [test_movies_task, test_ratings_task] >> spark_job_task

if __name__ == "__main__":
    dag.cli()
