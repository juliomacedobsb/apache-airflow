from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

def import_csv_to_postgres():
    # Caminho do arquivo CSV
    csv_file = '/home/cliente/Área de Trabalho/projetos/df_pandas1.csv'

    # Colunas do arquivo CSV
    columns = ["Classificação", "Cidade", "janeiro", "FEV", "MAR"]

    # Carregar o arquivo CSV para um DataFrame pandas
    df = pd.read_csv(csv_file, usecols=columns)

    # Conectar-se ao banco de dados Postgres usando o segredo
    postgres_conn = Variable.get("postgres_conn")
    engine = create_engine(postgres_conn)

    # Nome da tabela no banco de dados Postgres
    table_name = 'poluiçao'

    # Enviar o DataFrame para o banco de dados Postgres
    df.to_sql(table_name, engine, if_exists='replace', index=False, schema='public')

# Defina os argumentos do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 23)
}

# Definir o DAG
dag = DAG(
    'import_csv_to_postgres',
    schedule_interval=None,   # Executar a cada 10 minutos
    default_args=default_args
)

# Definir a tarefa do DAG
import_csv_task = PythonOperator(
    task_id='import_csv_task',
    python_callable=import_csv_to_postgres,
    dag=dag
)

# Definir a ordem das tarefas
import_csv_task



