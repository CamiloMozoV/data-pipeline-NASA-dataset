from datetime import datetime
import pendulum
from docker.types import Mount
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

locals_tz = pendulum.timezone("America/Bogota")

with DAG(
    dag_id="firesdata_pipeline",
    description="data pipeline for fires data NASA",
    start_date=datetime(year=2023, month=3, day=28, tzinfo=locals_tz),
    schedule="@daily",
    catchup=False,
    tags=["dev"]
):  
    MODIS_FILENAME = "MODIS-data.csv"
    VIIRS_FILENAME = "VIIRS-data.csv"
    SPARK_SRC="/opt/bitnami/spark/src"

    with TaskGroup(group_id="fetch_data_nasa") as fetch_data_nasa:
        """Fetch data from NASA website"""

        fetch_modis_data = DockerOperator(
            task_id="fetch_modis_data",
            api_version="auto",
            image="fetch-data-nasa",
            command=[
                # "fetch_data_nasa.py",
                "https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Global_24h.csv",
                "--filename",
                MODIS_FILENAME
            ],
            network_mode="airflow-pyspark",
            mounts=[
                Mount(source="/tmp/dockerdata/", target="/tmp/", type="bind")
            ],
            auto_remove="force"
        )

        fetch_viirs_data = DockerOperator(
            task_id="fetch_viirs_data",
            api_version="auto",
            image="fetch-data-nasa",
            command=[
                # "fetch_data_nasa.py",
                "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv",
                "--filename",
                VIIRS_FILENAME
            ],
            network_mode="airflow-pyspark",
            mounts=[
                Mount(source="/tmp/dockerdata/", target="/tmp/", type="bind")
            ],
            auto_remove="force"
        )

    with TaskGroup(group_id="data_transformation") as data_transformation:
        """Perform the task related to data transformation"""

        modis_transformation = SparkSubmitOperator(
            task_id="modis_transformation",
            application=f"{SPARK_SRC}/MODIS_dataset_transformation.py",
            py_files=f"{SPARK_SRC}/utils.py",
            name="modis-transformation",
            conn_id="spark_conn_id",
            jars=f"/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.319.jar,"
                 f"/opt/bitnami/spark/jars/aws-java-sdk-1.12.319.jar,/opt/bitnami/spark/jars/hadoop-common-3.3.4.jar,"
                 f"/opt/bitnami/spark/jars/hadoop-client-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            num_executors=1
        )

        viirs_transformation = SparkSubmitOperator(
            task_id="viirs_transformation",
            application=f"{SPARK_SRC}/VIIRS_dataset_transformation.py",
            py_files=f"{SPARK_SRC}/utils.py",
            name="viirs-transformation",
            conn_id="spark_conn_id",
            jars=f"/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,/opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.319.jar,"
                 f"/opt/bitnami/spark/jars/aws-java-sdk-1.12.319.jar,/opt/bitnami/spark/jars/hadoop-common-3.3.4.jar,"
                 f"/opt/bitnami/spark/jars/hadoop-client-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
            num_executors=1
        )


    chain([fetch_modis_data, fetch_viirs_data],
          [modis_transformation, viirs_transformation])
