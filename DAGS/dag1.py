from airflow.models import DAG
from airflow.operators.dummy import DummyOperator #vacío sólo marca
from airflow.utils.dates import days_ago #para la defición del DAG >> start_date=days_ago(1)
from airflow.providers.postgres.operators.postgres import PostgresOperator #for stage 'prepare'
from airflow.providers.postgres.hooks.postgres import PostgresHook #for func ingest_data
from airflow.operators.sql import BranchSQLOperator #dividirá el camino para ver si está vacío en stage 

def ingest_data():
    hook = PostgresHook(postgres_conn_id="con_table")
    hook.insert_rows(

    )



with DAG ("db_ingestion",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False)as dag:

    start_workflow =DummyOperator (task_id = "start_workflow")
    validate=DummyOperator (task_id = "validate") #contar cuantas son
    prepare=PostgresOperator (task_id = "prepare_table", postgres_conn_id="con_table", 
        sql=""" 
        CREATE SCHEMA IF NOT EXISTS;
        CREATE TABLE if not exist.user_purchase (
        invoice_number varchar(10),
        stock_code varchar(20),
        detail varchar(1000),
        quantity int,
        invoice_date timestamp,
        unit_price numeric(8,3),
        customer_id int,
        country varchar(20)
        )
    """,
    )
 #postgres_conn_id="con_table"('Stage'prepare=PostgresOperator) lo defines en AIRFLOW (localhost:8080/home) 
 #en pestañas Admin/Connections/ +
 #En la nueva pestaña que se abre: 
 #Connection id : en esta ocasión nombrado <con_table>
 # Connection type: Postgres para esta ocasión
 # Host: lo saqué de AWS/RDS/Database/instance/ punto de enlace (ctrl+c)
 # schema : nombre de la bd que conectaremos, <user_purchase> en este caso 
 #login (username)
 #pass...
 
    clear = PostgresOperator( #clear data if is not empty... sale de branch
        task_id="clear",
        postgres_conn_id="ml_conn",
        sql="""DELETE FROM wize.user_purchase""",
    )
    continue_workflow = DummyOperator(task_id="continue_workflow") #continua el workflow si está empty
    branch = BranchSQLOperator (
        task_id='is_empty',
        conn_id='con_table',
        sql="SELECT COUNT(*) AS rows FROM user_purchase", 
        #if the result count is 0 mark as false, if is 1 or more is TRUE and clear.... 
        follow_task_ids_if_true=[clear.task_id], #llama a task clear 
        follow_task_ids_if_false=[continue_workflow.task_id], #llama a task continue_workflow
    )
    
    load=DummyOperator (task_id = "load")
    end_workflow=DummyOperator (task_id = "end_workflow")

#orden
start_workflow >> validate >> prepare >> load >>end_workflow  