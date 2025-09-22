import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook

import pandas as pd


def fetchDB(**kwargs):
    # Replace with your Airflow connection ID
    postgres_conn_id = 'postgres_default'
    sql_query = "SELECT * FROM auto_sales_order;"  # Replace with your SQL query

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()

    kwargs['ti'].xcom_push(key='data', value=data)


def dataCleaning(**kwargs):
    # fetch from previous task
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetchDB', key='data')
    columnNames = [
        'order_number', 'order_quantity', 'product_price', 'order_line_number', 'sales', 'order_date', 'days_since', 'order_status', 'product_line',
        'product_msrp', 'product_code', 'customer_name', 'phone', 'address', 'city', 'postal_code', 'country', 'contact_last_name', 'contact_first_name', 'deal_size'
    ]
    df = pd.DataFrame(data, columns=columnNames)

    # normalize columns
    df.columns = df.columns.str.lower().str.strip()

    colMap = {
        "ordernumber": "order_number",
        "quantityordered": "order_quantity",
        "priceeach": "product_price",
        "msrp": "product_msrp",
        "orderdate": "order_date",
        "orderlinenumber": "order_line_number",
        "days_since_lastorder": "days_since",
        "order_date": "order_date",
        "status": "order_status",
        "productline": "product_line",
        "productcode": "product_code",
        "customername": "customer_name",
        "addressline1": "address",
        "postalcode": "postal_code",
        "contactlastname": "contact_last_name",
        "contactfirstname": "contact_first_name",
        "dealsize": "deal_size",
    }
    df.rename(columns=colMap, inplace=True)

    # handle duplicate
    if df.duplicated().sum() > 0:
        df.drop_duplicates()

    # handle missing values
    result = df.isna().sum()
    values = []
    for idx, val in enumerate(result):
        if val > 0:
            values.append(result.keys().to_list()[idx])

    if len(values) > 0:
        df = df.dropna()

    # convert to datetime
    dfCopy = df.copy()

    dfCopy["order_date"] = pd.to_datetime(
        dfCopy["order_date"], format="%d/%m/%Y", errors="coerce")

    if dfCopy["order_date"].isna().sum() > 0:
        dfCopy = dfCopy.dropna(subset="order_date")

    # save to CSV
    df = dfCopy
    df.to_csv("/opt/airflow/data/cleaned_dataset.csv", index=False)


def saveToElastic():
    es_hosts = ["http://elasticsearch:9200"]
    es_hook = ElasticsearchPythonHook(hosts=es_hosts)
    es_client = es_hook.get_conn

    # read pickle sample
    df = pd.read_csv('/opt/airflow/data/cleaned_dataset.csv')

    # perform import
    for i, r in df.iterrows():
        doc = r.to_json()
        es_client.index(
            index="auto_sales_dataset",
            body=doc
        )


default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime.now(),
    # 'retries': 1,
    # 'retry_delay': dt.timedelta(minutes=3),
}


with DAG(
    'sql_to_elastic',
    default_args=default_args,
    # schedule_interval="10-30/10 9 * * 6"
) as dag:
    t1 = PythonOperator(
        task_id="fetchDB",
        python_callable=fetchDB
    )

    t2 = PythonOperator(
        task_id="dataCleaning",
        python_callable=dataCleaning
    )

    t3 = PythonOperator(
        task_id="saveToElastic",
        python_callable=saveToElastic
    )


t1 >> t2 >> t3
