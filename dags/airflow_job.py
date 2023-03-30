import json
from collections import defaultdict
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# In 1 hour (3600 s), 1200 entries are added since only
BATCHSIZE = 1200

# Schema for connecting to PostgreSQL
DB_HOST = 'postgres'
DB_SCHEMA = 'veryfidev'

def get_batch_start_id():
    '''
    This procedure returns the first document index to start the next batch processing.
    :return:
    '''
    # Create a hook to the Postgres DB
    pg_hook = PostgresHook(postgres_conn_id=DB_HOST, schema=DB_SCHEMA)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    # Retrieve the new batch_start_id using the "bookmark" table
    query = "SELECT * FROM bookmark;"
    cursor.execute(query)
    rows =  cursor.fetchall()
    batch_start_id = rows[0][0]
    
    return batch_start_id


def update_next_batch_id(next_batch_id):
    """
    This routine is evoked to update the "bookmark" table. The end of the current
    batch is stored. So that when the next batch processing happens, it can use 
    this value as a bookmark. To save space, UPDATE was used instead of INSERT since
    we intend to use it as a bookmark.

    Args:
        next_batch_id: integer value marking the end of current batch of records
    """
    # Create a Postgres hook to the DB
    pg_hook = PostgresHook(postgres_conn_id=DB_HOST, schema=DB_SCHEMA)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    # Update the "bookmark" table with the end of current batch
    query = f"UPDATE bookmark SET batch_size = {next_batch_id};"
    cursor.execute(query)
    pg_conn.commit()


def def_value():
    """
    Default dict values for every new business_id encountered
    """
    return {'total_value': 0, 'total_ocr_score': 0, 'total_ai_score': 0, 'num_total': 0}


def return_details(**kwargs):
    """
    Utility script to create dictionaries
    """
    return kwargs


def parse_total(total_response):
    """
    Parse the total response of each document retrieve from the "documents" table 
    and compute diagnostics like total number of documents, total value of sales,
    total OCR score of values, total AI score. Depending on the end-goal, more 
    statistics can be added

    Args:
        total_response: Dictionary containing the different document scans and bounding box 
                        information for each business

    Returns:
        A dict mapping each statistic to the corresponding value:
        {
        "total_value": 411170.0,
        "total_ocr_score": 3.22,
        "total_ai_score": 2.51,
        "num_total": 479
        }
    """
    total_value, total_ocr_score, total_ai_score, num_total = 0.0, 0.0, 0.0, 0

    if type(total_response) is dict:
        # if the total_response is a dictionary
        total_value += total_response['value']
        total_ocr_score += total_response['ocr_score']
        total_ai_score += total_response['score']
        num_total += 1
    elif type(total_response) is list:
        # if the total_response is a list of dictionaries
        for response in total_response:
            if response is not None:
                total_value += response['value']
                total_ai_score += response['score']
                total_ocr_score += response['ocr_score']
                num_total += 1

    return return_details(
        total_value=total_value,
        total_ocr_score=total_ocr_score,
        total_ai_score=total_ai_score,
        num_total=num_total
    )


def get_business_info(parsed_totals):
    """
    Performs business analytics: Scans a list of documents, retrieves the data, performs 
    analytics and stores it in a dictionary.

    Args:
        parsed_totals: List of strings, each of which is a dictionary containing the 
                        different document scans and bounding box information of receipt
                        scans of different businesses.

    Returns:
        Dictionary containing statistics for each business. For more details about what 
        statistics are computed refer to parse_total().
    """
    # Defining the dict for containing business information
    business_info = defaultdict(def_value)
    # Scan each document and perform the steps
    for doc_id, data in parsed_totals:
        # Translate the string to a dictionary
        ml_response = json.loads(data)
        business_id = ml_response['business_id']
        total_response = ml_response['total']
        # Perform analytics and store the data
        if total_response is not None:
            for k, v in parse_total(total_response).items():
                business_info[business_id][k] += v

    return business_info


def get_batch_data():
    """
    Queries the "documents" table to get the newer BATCHSIZE rows and calls the
    business analytics module to send processed data.

    Returns:
        Dictionary of business information. For more details, refer to get_business_info().
    """
    # Get the batch_start and batch_end for this batch of processing
    batch_start = get_batch_start_id()
    batch_end = batch_start + BATCHSIZE
    # Create a hook to the Postgres DB
    pg_hook = PostgresHook(postgres_conn_id=DB_HOST, schema=DB_SCHEMA)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    # Define the SQL query. Note that we use document_id > batch_start
    # since the document_id in "documents" table starts from 1
    query = "SELECT * FROM documents where document_id > "+ str(batch_start)
    query += " and document_id <= " + str(batch_end) + ";"
    cursor.execute(query)
    # Fetch the latest batch and do analysis on it
    parsed_totals = cursor.fetchall()
    # Update the next batch id
    update_next_batch_id(next_batch_id=batch_end)
    # Return the business_info
    return get_business_info(parsed_totals)


def write_batch_data(ti):
    """
    This procedure gets the business information from get_batch_data() 
    and writes it to the "parsed_total" table.

    Args:
        ti: XCom per-task-instance designed for communication within a DAG run.
            Helps to retrieve the business information sent by get_batch_data()
    """
    # Retrieve the business information
    docs = ti.xcom_pull(task_ids=['get_batch_data'])
    if not docs:
        raise Exception('No docs to process')
    business_info = docs[0]
    # Create a hook onto the databses
    pg_hook = PostgresHook(postgres_conn_id=DB_HOST, schema=DB_SCHEMA)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    # Obtain business information for each business_id and write them to the 
    # "parsed_table" table. This will contain information of business performed
    # in the last hour or last day
    for business_id, info in business_info.items():
        query = """INSERT INTO parsed_total (business_id, num_total, total_value, total_ocr_score, total_ai_score, time_stamp) VALUES(%s, %s, %s, %s, %s, %s);"""
        cursor.execute(query,
                       (business_id, int(info['num_total']), info['total_value'], info['total_ocr_score'], info['total_ai_score'], datetime.now())
                       )
    pg_conn.commit()


# Set the DAG for the Airflow job. This job will be executed once per hour or per day. 
# Currently it is configured to run once per hour and can be modified. 
dag = DAG(
    dag_id='airflow_job',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(year=2023, month=3, day=29),
    catchup=False
)

task_get_batch_data = PythonOperator(
    task_id='get_batch_data',
    python_callable=get_batch_data,
    do_xcom_push=True,
    dag=dag
)

task_write_batch_data = PythonOperator(
    task_id='write_batch_data',
    python_callable=write_batch_data,
    dag=dag
)
# Set the execution order of sequential tasks using the Airflow DAG
task_get_batch_data >> task_write_batch_data