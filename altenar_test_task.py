from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from os import system, listdir, remove
import subprocess, gzip, shutil, logging
from datetime import datetime, timedelta

logger = logging.getLogger()

def spark_submit(**kwargs):
    '''
    Function imitating spark-submit command.
    :kwargs - contains keys:
        py_file - path to python file to submit
        args - args to provide into driver program
    '''
    cmd = f"spark-submit {kwargs['py_file']}"
    # pars args and and them to spark-submit string
    if 'args' in kwargs:
        for arg in kwargs['args']:
            cmd += f' {arg}'
    # build subprocess context
    sp = subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    # Separate the output and error.
    stdout, stderr = sp.communicate()
    # Log spark stdout and stderr
    logger.error(stderr)
    logger.info(stdout)


def download_file():
    '''
    Function uses wget command to download GA files for previous day
    '''
    #compute previous day
    available_date = datetime.today() - timedelta(days=1)
    request_date = str(available_date.date())
    # create wget command
    wget_str = 'wget https://data.gharchive.org/%s-{1..23}.json.gz -P /home/ubuntu/altenar_test_data' % (request_date)
    # execute wget commant with curly braces using bash
    system(f"/bin/bash -c '{wget_str}'")
    return request_date

def unzip_file():
    '''
    Function unzips gz archives and loads unziped files to hdfs for further processing
    '''
    files_to_parse = []
    # temporary directory to place unziped files 
    base_dir = '/home/ubuntu/altenar_test_data/'
    for gz_file in listdir(base_dir):
        gz_file_path = base_dir + gz_file
        json_file_name = gz_file[:-3]
        json_file_path = base_dir + json_file_name
        # open archive and unzip it
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(base_dir + json_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        # use shell command to upload file to hdfs
        system(f'hdfs dfs -put {json_file_path} /user/ubuntu/altenar_test_data/')
        files_to_parse.append(json_file_name)
        remove(gz_file_path)
        remove(json_file_path)

    return files_to_parse

def parse_jsons_to_parquet(ti):
    # pull xcom passed data
    files_to_parse = ti.xcom_pull(task_ids=['unzip_file'])[0]
    submit_args = {
        'py_file': '/home/ubuntu/spark_jobs/json_to_parquet.py',
        'args': files_to_parse
    }
    spark_submit(**submit_args)
    # use shell command to clear directory holding json files in hdfs as we don't need them anymore
    system('hdfs dfs -rm /user/ubuntu/altenar_test_data/*')
    
def report_to_csv(ti):
    # pull xcom passed data
    request_date = ti.xcom_pull(task_ids=['download_file'])[0]
    submit_args = {
        'py_file': '/home/ubuntu/spark_jobs/report_to_csv.py',
        'args': [request_date]
    }
    spark_submit(**submit_args)
    for rep_num in range(1, 4):
        # use shell command to copy result reports
        system('hdfs dfs -copyToLocal /user/ubuntu/report_%s-%s.csv /home/ubuntu/altenar_reports' % (rep_num, request_date))

with DAG(
    'altenar_task_dag', 
    schedule_interval='0 1 * * *',
    start_date=datetime(2022, 12, 22), 
    catchup=False
) as dag:
    
    download_file_operator = PythonOperator(task_id='download_file', python_callable=download_file, do_xcom_push=True)
    
    unzip_file_operator = PythonOperator(task_id='unzip_file', python_callable=unzip_file, do_xcom_push=True)
    
    parse_jsons_to_parquet_operator = PythonOperator(task_id='parse_jsons_to_parquet', python_callable=parse_jsons_to_parquet)

    report_to_csv_operator = PythonOperator(task_id='report_to_csv', python_callable=report_to_csv)

download_file_operator >> unzip_file_operator >> parse_jsons_to_parquet_operator >> report_to_csv_operator