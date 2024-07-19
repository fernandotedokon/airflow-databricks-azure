from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
  'airflow_databricks_azure', description="Airflow API Databricks Azure",
  start_date=datetime(2024, 7, 15), 
  schedule_interval="0 2 * * *",  # Todos os dias as 2 da manhã
  ) as dag_executando_notebook_extracao:
    
    extracting_data = DatabricksRunNowOperator(
    task_id = 'extracting_data',
    databricks_conn_id = 'databricks_default',
    job_id = 727100075440173,
    notebook_params={"data_execucao": '{{data_interval_end.strftime("%Y-%m-%d")}}'}
  )
    
    transformation_end = DatabricksRunNowOperator(
    task_id = 'transformation_end',
    databricks_conn_id = 'databricks_default',
    job_id = 705717944275217
  )

    automate_reporting = DatabricksRunNowOperator(
    task_id = 'automate_reporting',
    databricks_conn_id = 'databricks_default',
    job_id = 265770741440707
  )

    extracting_data >> transformation_end >> automate_reporting