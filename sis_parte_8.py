# Importaciones de Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago

# Otras importaciones
from datetime import timedelta

# Importaciones locales
from app.sis.common import truncate_mysql_table
from app.sis.common import get_variables

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

# Llamar a la función y obtener las variables
config_variables = get_variables(params=[
    "mysql_connection_id",
    "DA_AfectadaEdadGenero",
    "solicitudes",
    "afectada",
    "avances",
    "avanceestados",
    "metadatos",
    "tipometadato",
    "tipolugar",
    "areas",
    "canal",
    "tipopeticions"])

# Acceder a las variables
mysql_connection_id = config_variables["mysql_connection_id"]
DA_AfectadaEdadGenero_table_name = next(iter(config_variables["DA_AfectadaEdadGenero"].values()))[0]
solicitudes_table_name = next(iter(config_variables["solicitudes"].values()))[0]
afectada_table_name = next(iter(config_variables["afectada"].values()))[0]
avances_table_name = next(iter(config_variables["avances"].values()))[0]
avanceestados_table_name = next(iter(config_variables["avanceestados"].values()))[0]
metadatos_table_name = next(iter(config_variables["metadatos"].values()))[0]
tipometadato_table_name = next(iter(config_variables["tipometadato"].values()))[0]
tipolugar_table_name = next(iter(config_variables["tipolugar"].values()))[0]
areas_table_name = next(iter(config_variables["areas"].values()))[0]
canal_table_name = next(iter(config_variables["canal"].values()))[0]
tipopeticions_table_name = next(iter(config_variables["tipopeticions"].values()))[0]

# Definición del DAG
with DAG(
    dag_id="sis-parte-08",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags = ["sis", "datos"]
) as dag:

    truncate_mysql_table_task = PythonOperator(
        task_id="truncate_mysql_table_task",
        python_callable=truncate_mysql_table,
        op_kwargs={'table': DA_AfectadaEdadGenero_table_name, 'connection_id': mysql_connection_id},
    )

    insert_mysql_task = MySqlOperator(
        task_id="insert_mysql_task",
        mysql_conn_id=mysql_connection_id,
        sql=f"""
        INSERT INTO {DA_AfectadaEdadGenero_table_name} (
            `id_solicitud`,
            `avance_estado`,
            `fecha_avance`,
            `tipo_metadato`,
            `metadato`,
            `edad`,
            `afectada_genero`,
            `id_afectada`,
            `afectada_nacionalidad`,
            `afectada_ocupacion`,
            `afectada_rango_etario`,
            `afectada_rango_etario_infantil`
        )
        SELECT
            s._id AS id_solicitud,
            ave.nombre AS avance_estado,
            av.fecha AS fecha_avance,
            tm.nombre AS tipo_metadato,
            m.detalle AS metadato,
            af.edad,
            af.genero AS afectada_genero,
            af._id AS id_afectada,
            af.nacionalidad AS afectada_nacionalidad,
            af.ocupacion AS afectada_ocupacion,
            af.rango_etario,
            af.rango_etario_infantil
        FROM
            nc_twf6___solicitudes s
            INNER JOIN {afectada_table_name} af ON s._id = af.solicitud_id
            LEFT JOIN {avanceestados_table_name} av ON s._id = av.solicitud_id
            LEFT JOIN {avanceestados_table_name} ave ON av.estado = ave._id
            LEFT JOIN {metadatos_table_name} m ON av._id = m.avance_id
            LEFT JOIN {tipometadato_table_name} tm ON m.tipometadato_id = tm._id;
        """,
    )

    update_mysql_task = MySqlOperator(
        task_id="update_mysql_task",
        mysql_conn_id=mysql_connection_id,
        sql=f"""
        UPDATE {DA_AfectadaEdadGenero_table_name} DA
        JOIN {solicitudes_table_name} s ON DA.id_solicitud = s._id
        LEFT JOIN {tipolugar_table_name} tl ON s.tipolugar_id = tl._id
        LEFT JOIN {areas_table_name} a ON s.area_id = a._id
        LEFT JOIN {areas_table_name} pa ON a.parent = pa._id
        LEFT JOIN {canal_table_name} c ON s.canal_id = c._id
        LEFT JOIN {tipopeticions_table_name} st ON s.subtipo_id = st._id
        LEFT JOIN {tipopeticions_table_name} t ON st.tipo = t._id
        SET
            DA.fecha = s.fecha,
            DA.hora = s.hora,
            DA.fechaIngreso = s.fechaIngreso,
            DA.ultimo_estado = s.estado,
            DA.fechaUltimoAvance = s.fechaUltimoAvance,
            DA.lugar = s.lugar,
            DA.tipolugar = tl.nombre,
            DA.area = a.nombre,
            DA.canal = c.nombre,
            DA.parent_area = pa.nombre,
            DA.tipo = t.nombre,
            DA.subtipo = st.nombre,
            DA.urgencia = s.urgencia;
        """,
    )

# Definición del flujo del DAG
truncate_mysql_table_task >> insert_mysql_task >> update_mysql_task