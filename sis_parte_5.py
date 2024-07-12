# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Otras importaciones
from datetime import timedelta
import logging
import json

# Importaciones locales
from app.sis.common import get_mongo_collection, truncate_mysql_table, get_variables


# Definición de argumentos por defecto para el DAG
default_args = {
    "owner": "Datos MJUS",
    "depends_on_past": False,
    "email": ["depto.datos@mjus.gba.gob.ar"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "task_concurrency": 1
}

tabla="afectada"
# Llamar a la función y obtener las variables
config_variables = get_variables(params=[tabla,"mongo_connection_id","mongo_database_name","mysql_connection_id"])

# Acceder a las variables
mongo_connection_id = config_variables["mongo_connection_id"]
mongo_database_name = config_variables["mongo_database_name"]
mysql_connection_id = config_variables["mysql_connection_id"]
table = config_variables[tabla]

# Obtenemos el nombre de la coleccion de MongoDB, el nombre de la tabla de MySQL y su estructura (columnas)
mongo_collection_name, mysql_table_info = next(iter(table.items()))
mysql_table_name, mysql_table_columns = mysql_table_info


def insert_table_task(collection, columns, table, connection_id):
    """
    Inserta datos en una tabla en MySQL.

    Args:
        collection (str): El nombre de la variable de Airflow que contiene los documentos JSON.
        columns (list): Lista de columnas que se van a insertar en la tabla MySQL.
        table (str): El nombre de la tabla en MySQL.
        connection_id (str): El ID de la conexión de MySQL en Airflow.

    Returns:
        None
    """

    # Obtener documentos JSON de la Variable de Airflow
    documents = json.loads(Variable.get(collection))

    filas = []

    # Iterar sobre los documentos y preparar las filas para la inserción
    try:
        for registro in documents:

            # Creación del diccionario con los campos aplanados
            diccionario = {
                "provincia": registro.get("domicilio", {}).get("provincia", None),
                "localidad": registro.get("domicilio", {}).get("localidad", None),
                "direccion": registro.get("domicilio", {}).get("direccion", None),
                "generoId": registro.get("infoDemografica", {}).get("generoId", None),
                "nacionalidad": registro.get("infoDemografica", {}).get("nacionalidad", None),
                "edad": registro.get("infoDemografica", {}).get("edad", None),
                "ocupacion": registro.get("infoDemografica", {}).get("ocupacion", None),
                "infoDemograficaId": registro.get("infoDemografica", {}).get("_id", None)
            }

            # Agregar los campos adicionales del registro al diccionario
            for each in columns:
                if each not in diccionario:
                    diccionario[each] = registro.get(each, None)
            
            # Convertir el diccionario en una lista ordenada de valores para su posterior inserción
            fila = [diccionario[columna] for columna in columns]
            filas.append(fila)
    except Exception as e:
        logging.error("Ocurrió un error:", exc_info=True)

    # Crear el hook de MySQL
    mysql_hook = MySqlHook(mysql_conn_id=connection_id)

    # Inserción de filas en lotes
    try:
        if filas:
            mysql_hook.insert_rows(
                table=table,
                rows=filas,
                target_fields=columns,
                commit_every=1000
            )
        else:
            logging.warning(f'No se encontraron filas para insertar en la tabla {table}')
    except Exception as e:
        logging.error(f'Error ejecutando la inserción de filas en lotes. Error completo: "{e}"')


# Definición del DAG
with DAG(
    dag_id="sis-parte-05",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags = ["sis", "mongo", "datos"]
) as dag:

    get_mongo_collection_task = PythonOperator(
        task_id='get_mongo_collection_task',
        python_callable=get_mongo_collection,
        op_kwargs={'collection': mongo_collection_name, 'connection_id': mongo_connection_id, 'database_name': mongo_database_name},
    )

    truncate_mysql_table_task = PythonOperator(
                task_id=f'truncate_mysql_table_task',
                python_callable=truncate_mysql_table,
                op_kwargs={'table': mysql_table_name, 'connection_id': mysql_connection_id},
    )

    insert_mysql_task = PythonOperator(
        task_id='insert_table',
        python_callable=insert_table_task,
        op_kwargs={'collection': mongo_collection_name, 'columns': mysql_table_columns, 'table': mysql_table_name, 'connection_id': mysql_connection_id},
    )
    
    # Definición del flujo del DAG
    [get_mongo_collection_task, truncate_mysql_table_task] >> insert_mysql_task