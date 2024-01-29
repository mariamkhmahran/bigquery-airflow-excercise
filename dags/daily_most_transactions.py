from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta

CONN_ID = 'gcp'
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


query_template = """
SELECT
  DATE(block_timestamp) AS date,
  from_address,
  COUNT(*) AS transaction_count
FROM
  `bigquery-public-data.crypto_ethereum_classic.transactions`
WHERE
    receipt_status = 1
GROUP BY
  date, from_address
ORDER BY
  date DESC
"""

with DAG(
    'eth_analysis_dag',
    default_args=default_args,
    description='DAG to find user who sent the most successful transactions per day', 
) as dag:
    create_new_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='new_dataset_creator',
        dataset_id='eth_analysis',
        project_id='lunar-inn-412616',
        dataset_reference={"Eth_analysis": "New Dataset"},
        gcp_conn_id=CONN_ID,
        dag=dag
    )

    CreateTable = BigQueryCreateEmptyTableOperator(
        task_id="new_table_creator",
        dataset_id='eth_analysis',
        table_id="daily_most_transactions",
        project_id='lunar-inn-412616',
        gcp_conn_id=CONN_ID,
        schema_fields=[
            { "name": "date", "type": "DATE" },
            { "name": "from_address", "type": "STRING"},
            { "name": "transaction_count", "type": "INTEGER"},
        ],
    )

    bq_operator = BigQueryExecuteQueryOperator(
        task_id='run_query',
        sql=query_template,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='eth_analysis.daily_most_transactions',
        dag=dag,
        gcp_conn_id=CONN_ID
    )

    create_new_dataset >> CreateTable >> bq_operator
