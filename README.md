# Nova Drive data engineering project.

### The Nova Drive project is a data engineering project that uses a database in the Postgres database to simulate the sales system of a car dealership. This database is made available through the course [access link](https://www.udemy.com/course/bootcamp-engenharia-de-dados/) taught by Fernando Amaral in his data engineering bootcamp.

## Description

#### The project basically consists of two stages: the first is the data engineering layer and the second is the data analysis layer. In the first layer, data is collected from a postgres database of a fictitious car dealership and is stored in a raw layer in the snowflake database using Apache Airflow as the data ingestion mechanism. DBT is then used to process the data for the staging and dw layers in Snowflake. In the second layer, an analytics view is created also using DBT with specific objectives for a data analysis project developed in Power BI. The goal is to present an end-to-end solution using some of the main modern data engineering and analysis tools.
&nbsp;
![alt text](images/diagram.jpeg "project diagram")
&nbsp;

## Data engineering layer

#### The data engineering layer can be divided into a few steps. They are: Data ingestion by Airflow; creation of the raw layer in the Snowflake database; and data transformation using DBT.

### Data ingestion by Airflow

#### To configure Apache Airflow, a Docker container was initialized on an instance created on AWS EC2. The configurations were made so that Airflow could connect to the Postgres database, identify the tables needed for the process and load them into the Snowflake database.
&nbsp;
![alt text](Airflow%20layer/images/EC2%20Instance.png "AWS EC2 instance")
&nbsp;

#### Loading data from Postgres database tables is done in full load mode. Below is the Python code for the functions contained in the DAG that performs this process. The first function is exclusively for loading data into Airflow memory and the second function performs the necessary load on the raw tables of the Snowflake database.
&nbsp;
```python
def extract_table(table_name, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    query = f"SELECT * FROM {table_name}"
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=col_names)
    
    for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']):
        df[col] = df[col].astype(str)
    
    kwargs['ti'].xcom_push(key=f"{table_name}_data", value=df.to_dict(orient='records'))
```

&nbsp;
```python
def load_table(table_name, **kwargs):
    table_data = kwargs['ti'].xcom_pull(key=f"{table_name}_data", task_ids=f"extract_{table_name}")
    df = pd.DataFrame(table_data)
    
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake")
    engine = snowflake_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, schema='RAW', if_exists='replace', index=False)
```

#### The DAG execution is demonstrated in the images below. Each table represents a different execution within the flow. Since this is a simple full load, only one DAG is necessary in this loading process.

&nbsp;
![alt text](Airflow%20layer/images/dag%20graph.png "DAG steps")
&nbsp;
&nbsp;
![alt text](Airflow%20layer/images/dag.png "DAG execution")
&nbsp;

### Snowflake raw database