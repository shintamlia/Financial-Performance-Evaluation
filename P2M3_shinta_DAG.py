'''
=================================================
Milestone 3 - DAG

Nama  : Shinta Amalia
Batch : FTDS-034-RMT
Deskripsi
Program ini dibuat untuk melakukan otomatisasi proses Extract, Transform, Load (ETL) dari PostgreSQL ke ElasticSearch. Dataset yang digunakan adalah dataset penjualan produk minuman suatu perusahaan pada periode tahun 2010 sampai dengan 2011
=================================================
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from elasticsearch import Elasticsearch, helpers

def fetch_data_from_postgresql():
    '''
    Mengambil data dari database PostgreSQL dan menyimpannya ke file CSV sementara.

    Fungsi ini melakukan koneksi ke database PostgreSQL menggunakan `psycopg2`,
    mengeksekusi query SQL untuk mengambil semua data dari tabel `table_m3`, 
    dan menyimpannya sebagai file CSV sementara.
    '''
    # database connection
    conn = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    
    # query
    query = "select * from table_m3"
    df = pd.read_sql_query(query, conn)
    
    conn.close()

    # save to csv
    df.to_csv("/opt/airflow/csv/P2M3_shinta_data_raw.csv", index=False)

def clean_data():
    '''
    Membersihkan data pada file CSV sementara dan menyimpannya ke file CSV lain.

    Fungsi ini melakukan pembersihan data dengan langkah-langkah berikut:
    - Menghapus baris yang duplikat.
    - Normalisasi nama kolom menjadi lowercase dan mengganti spasi dengan underscore.
    - Menghapus baris yang memiliki missing values.
    '''
    # read csv
    df = pd.read_csv("/opt/airflow/csv/P2M3_shinta_data_raw.csv")

    # delete duplicate data
    df.drop_duplicates(inplace=True)
    
    # normalisasi nama kolom
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace(r'\W', '', regex=True)
    
    # drop missning value
    df.dropna(inplace=True)

    # save to csv
    df.to_csv("/opt/airflow/csv/P2M3_shinta_data_clean.csv", index=False)

def post_to_elasticsearch():
    '''
    Memposting data dari file CSV ke ElasticSearch.

    Fungsi ini membaca data dari file CSV yang sudah dibersihkan dan memposting setiap baris
    sebagai dokumen ke index ElasticSearch.
    '''
    # read csv
    df = pd.read_csv("/opt/airflow/csv/P2M3_shinta_data_clean.csv")

    # Elasticsearch connection
    es = Elasticsearch([{"host": "elasticsearch",
                         "port": "9200"}])
    
    # function generate_data
    def generate_data(df):
        for index, row in df.iterrows():
            yield {
                "_index": "table_m3",
                "_id": index,
                "_source": row.to_dict(),
            }

    # post to Elasticsearch
    helpers.bulk(es, generate_data(df))

# DAG argument
default_args = {
    "owner": "shinta",
    "start_date": datetime(2024, 8, 15, 6, 30, 0) - timedelta(hours=7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# init DAG
with DAG(
    "P2M3_data_pipeline",
    description="DAG untuk fetch data dari postgresql, cleaning data, export clean data to csv dan posting ke Elasticsearch",
    schedule_interval="30 6 * * *",
    default_args=default_args, 
    catchup=False
) as dag:

    # task_1 get data from postgresql
    task_fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data_from_postgresql
    )

    # task_2 clean data and save
    task_clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    # task_3 post to elasticsearch
    task_post_to_elasticsearch = PythonOperator(
        task_id="post_to_elasticsearch",
        python_callable=post_to_elasticsearch
    )

    # task order
    task_fetch_data >> task_clean_data >> task_post_to_elasticsearch