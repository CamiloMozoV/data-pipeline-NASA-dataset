from airflow.models.connection import Connection

spark_conn = Connection(
    conn_id="spark_conn_id",
    conn_type="spark",
    host="spark-master",
    port=7077,
    extra={"queue": "root.default", 
           "deploy_mode": "cluster", 
           "spark_home": "/opt/bitnami/spark/", 
           "spark_binary": "spark-submit"
        }
)

print(f"AIRFLOW_CONN_{spark_conn.conn_id.upper()}={spark_conn.get_uri()}")