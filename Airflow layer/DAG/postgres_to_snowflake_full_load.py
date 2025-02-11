from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Listagem das tabelas
tables = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']

# Função para extrair dados de uma tabela do PostgreSQL
def extract_table(table_name, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    query = f"SELECT * FROM {table_name}"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=col_names)
    
    # Converter colunas datetime para string (serializável)
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']):
        df[col] = df[col].astype(str)
    
    # Salvar os dados no XCom
    kwargs['ti'].xcom_push(key=f"{table_name}_data", value=df.to_dict(orient='records'))

# Função para carregar dados de uma tabela no Snowflake
def load_table(table_name, **kwargs):
    table_data = kwargs['ti'].xcom_pull(key=f"{table_name}_data", task_ids=f"extract_{table_name}")
    df = pd.DataFrame(table_data)
    
    # Inserir dados no Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake")
    engine = snowflake_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, schema='RAW', if_exists='replace', index=False)

# Definições da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_snowflake_full_load",
    default_args=default_args,
    description="DAG para carregar múltiplas tabelas do PostgreSQL para Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Criar dinamicamente as tarefas
    for table in tables:
        extract_task = PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table,
            op_kwargs={"table_name": table},
        )

        load_task = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table,
            op_kwargs={"table_name": table},
        )

        # Definir a sequência de execução
        extract_task >> load_task

