from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pgcopy
from io import StringIO
import psycopg2
import pandas as pd
import tarfile
import csv

destination_path = ('/Users/celestin/airflow/dags/')
pg_credentials = {
    "dbname": "temp_db",
    "user": "Celestin",
    "password": "10august",
    "host": "localhost",
    "port": "5432"
}

def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", "r:gz") as tar:
        tar.extractall(path=f"{destination_path}")

def extract_data_from_csv():
    """ Extracts rowid, timestamp, vehicle number and vehicle type from the 'vehicle-data.csv' file
    and saves it into 'csv_data.csv' """
    input_file = f"{destination_path}/vehicle-data.csv"
    output_file = f"{destination_path}/csv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])
        for line in infile:
            row = line.split(',')
            writer.writerow([row[0], row[1], row[2], row[3]])

def extract_data_from_tsv():
    """ Extracts axles count, tollplaza id, and tollplaza code from the tollplaza-data.tsv file
    and save it into 'tsv_data.csv' """
    input_file = f"{destination_path}/tollplaza-data.tsv"
    output_file = f"{destination_path}/tsv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        for line in infile:
            row = line.split('\t')
            writer.writerow([row[0], row[1], row[2]])

def extract_data_from_txt():
    """" Extracts payment code and vehicle code from 'payment-data.txt'
     and saves it into a file named 'fixed_width_data.csv' """
    input_file = f"{destination_path}/payment-data.txt"
    output_file = f"{destination_path}/fixed_width_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Type of Payment code', 'Vehicle Code'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])

def consolidate_data():
    """ Merges the extracted files into 'extracted_data.csv' """
    csv_file = f"{destination_path}./csv_data.csv"
    tsv_file = f"{destination_path}./tsv_data.csv"
    txt_file = f"{destination_path}./fixed_width_data.csv"
    output_file = f"{destination_path}./extracted_data.csv"
    with (open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(txt_file, 'r') as fixed_in,
          open(output_file, 'w') as out_file):
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles',
                         'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code'])
        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)
        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)

def transform_data():
    """ Changes 'vehicle type' column to uppercase format """
    input_file = f"{destination_path}/extracted_data.csv"
    output_file = f"{destination_path}/transformed_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Vehicle type'] = row['Vehicle type'].upper()
            writer.writerow(row)

def load_to_db():
    """ Loads 'transformed_data.csv' into postgresql db """
    try:
        with psycopg2.connect(**pg_credentials) as conn:
            df = pd.read_csv(f"{destination_path}/transformed_data.csv")
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=True)  
            csv_buffer.seek(0)
            with conn.cursor() as cur:
                next(csv_buffer)
                cur.copy_from(csv_buffer, 'etl_transformed_data', sep=',', null='NULL')
            conn.commit()
    except Exception as e:
        print(f"Exception occurred: {e}")
        raise 
    finally:
        if 'engine' in locals():
            engine.dispose()

default_args = {
    'owner': 'Celestin',
    'start_date': days_ago(0),
    'email': ['celestinpd@icloud.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# # DAG Definition
dag = DAG(
    'airflow_etl_pipeline',
    default_args=default_args,
    description='Python ETL Pipeline',
    schedule_interval=timedelta(days=1),
)

# # Task Definition
untar = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)
extract_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)
extract_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
extract_txt = PythonOperator(
    task_id='extract_data_from_txt',
    python_callable=extract_data_from_txt,
    dag=dag,
)
consolidate = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)
transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
load = PythonOperator(
    task_id='load_data',
    python_callable=load_to_db,
    dag=dag
)

# # Pipeline Definition
untar >> [extract_csv, extract_tsv, extract_txt] >> consolidate >> transform >> load
