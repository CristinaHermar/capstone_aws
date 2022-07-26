from airflow.models import DAG
from airflow.operators.dummy import DummyOperator #vacío sólo marca
from airflow.utils.dates import days_ago #para la defición del DAG >> start_date=days_ago(1)
from airflow.providers.postgres.operators.postgres import PostgresOperator #for stage 'prepare'
 
with DAG ("db_ingestion",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False)as dag:

    start_workflow =DummyOperator (task_id = "start_workflow")
    validate=DummyOperator (task_id = "validate") #contar cuantas son
    prepare=PostgresOperator (task_id = "prepare_table", postgres_conn_id="con_table", 
        sql=""" CREATE SCHEMA IF NOT EXIST;
CREATE TABLE if not exist.user_purchase (
   invoice_number varchar(10),
   stock_code varchar(20),
   detail varchar(1000),
   quantity int,
   invoice_date timestamp,
   unit_price numeric(8,3),
   customer_id int,
   country varchar(20)
);
"""
    ,
    )
    
    

    load=DummyOperator (task_id = "load")
    end_workflow=DummyOperator (task_id = "end_workflow")

#orden
start_workflow >> validate >> prepare >> load >>end_workflow  