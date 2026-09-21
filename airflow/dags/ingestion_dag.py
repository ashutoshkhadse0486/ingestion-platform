"""Airflow DAG that orchestrates the Beam-based ingestion workflow."""

from pathlib import Path
import json
import logging
import os
import subprocess
import time

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import psycopg2

BEAM_JAR = Path('/opt/data/beam-jars/beam-ingestion-pipeline-bundled.jar')
logger = logging.getLogger(__name__)

def run_beam_ingestion(**context):
    """Execute the bundled Beam jar for each source file in the current DAG run."""
    dag_run = context['dag_run']
    configuration = dag_run.conf or {}
    execution_id = configuration.get('execution_id')
    data_file_locations = configuration.get('data_file_locations')
    if not data_file_locations:
        data_file_locations = [configuration.get('data_file_location')]
    file_type = configuration.get('file_type')

    if not execution_id:
        raise AirflowException('A valid execution_id is required')

    logger.info('Starting Beam task: execution_id=%s, file_type=%s, data_file=%s', execution_id, file_type, data_file_locations)

    start_time = time.monotonic()
    for file_number, data_file_location in enumerate(data_file_locations, start=1):
        logger.info('Starting Beam split: execution_id=%s, file_number=%s, file=%s', execution_id, file_number, data_file_location)
        command = ['java', '-jar', str(BEAM_JAR), f'--filePath={data_file_location}', f'--executionId={execution_id}', f'--fileType={file_type}', f'--databasePassword={os.environ["POSTGRES_PASSWORD"]}']

        beam_process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for output_line in beam_process.stdout:
            logger.info('Beam: %s', output_line.rstrip())
        return_code = beam_process.wait()
        if return_code != 0:
            logger.info('Beam process failed: execution_id=%s, file=%s, exit_code=%s', execution_id, data_file_location, return_code)
            raise AirflowException(f'Beam process failed with exit code {return_code}')

    logger.info('Beam task completed: execution_id=%s, duration_seconds=%.2f', execution_id, time.monotonic() - start_time)

def get_configuration(context):
    """Return the DAG run configuration payload passed by the API."""
    return context['dag_run'].conf or {}

def open_metadata_record(**context):
    """Create a run record in ingestion_metadata before the processing job starts."""
    configuration = get_configuration(context)
    execution_id = configuration.get('execution_id')
    logger.info('Opening ingestion metadata: execution_id=%s', execution_id)
    connection = psycopg2.connect(host='postgres', port=5432, dbname='capstone', user='postgres', password=os.environ['POSTGRES_PASSWORD'])
    try:
        with connection.cursor() as cursor:
            cursor.execute("""INSERT INTO ingestion_metadata (execution_id, source_file_location, file_type, expected_record_count, status, started_at) VALUES (%s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP) ON CONFLICT (execution_id) DO NOTHING""", (configuration.get('execution_id'), json.dumps(configuration.get('data_file_locations', [configuration.get('data_file_location')])), configuration.get('file_type'), configuration.get('expected_record_count')))
        connection.commit()
        logger.info('Ingestion metadata marked RUNNING: execution_id=%s', execution_id)
    finally:
        connection.close()

def validate_record_count(**context):
    """Check that Beam loaded the expected number of records."""
    configuration = context['dag_run'].conf or {}
    execution_id = configuration.get('execution_id')
    expected_record_count = configuration.get('expected_record_count')

    logger.info('Checking ingestion record count: execution_id=%s, expected=%s', execution_id, expected_record_count)

    if expected_record_count is None:
        raise AirflowException('A valid expected_record_count is required')

    connection = psycopg2.connect(host='postgres', port=5432, dbname='capstone', user='postgres', password=os.environ.get('POSTGRES_PASSWORD'))
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM target_warehouse WHERE execution_id = %s', (execution_id,))
            actual_record_count = cursor.fetchone()[0]
    finally:
        connection.close()

    if actual_record_count != int(expected_record_count):
        logger.info('Record count mismatch: execution_id=%s, expected=%s, actual=%s', execution_id, expected_record_count, actual_record_count)
        raise AirflowException(f'Record count mismatch: expected {expected_record_count}, ' f'but loaded {actual_record_count}')

    logger.info('Record count validated: execution_id=%s, actual=%s', execution_id, actual_record_count)

def close_metadata_record(**context):
    configuration = get_configuration(context)
    execution_id = configuration.get('execution_id')
    expected_record_count = configuration.get('expected_record_count')
    validation_task = context['dag_run'].get_task_instance(task_id='validate_record_count')
    status = 'COMPLETED' if validation_task and validation_task.state == 'success' else 'FAILED'

    logger.info('Finalizing ingestion metadata: execution_id=%s, status=%s', execution_id, status)

    connection = psycopg2.connect(host='postgres', port=5432, dbname='capstone', user='postgres', password=os.environ['POSTGRES_PASSWORD'])
    try:
        with connection.cursor() as cursor:
            cursor.execute("""UPDATE ingestion_metadata SET successful_record_count = (SELECT COUNT(*) FROM target_warehouse WHERE execution_id = %s), expected_record_count = %s, status = %s, completed_at = CURRENT_TIMESTAMP WHERE execution_id = %s""", (execution_id, expected_record_count, status, execution_id))
        connection.commit()
        logger.info('Ingestion metadata finalized: execution_id=%s, status=%s', execution_id, status)
    finally:
        connection.close()

default_args = {'owner': 'ingestion_platform', 'start_date': days_ago(1), 'retries': 0}

with DAG(
    dag_id='data_ingestion_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Orchestrates the Beam ingestion pipeline'
) as dag:

    start_pipeline = PythonOperator(
        task_id='start_ingestion_metadata',
        python_callable=open_metadata_record,
    )

    run_beam_pipeline = PythonOperator(
        task_id='run_java_beam_ingestion',
        python_callable=run_beam_ingestion,
    )

    check_record_count = PythonOperator(
        task_id='validate_record_count',
        python_callable=validate_record_count,
    )

    finish_pipeline = PythonOperator(
        task_id='finish_ingestion_metadata',
        python_callable=close_metadata_record,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_pipeline >> run_beam_pipeline >> check_record_count >> finish_pipeline