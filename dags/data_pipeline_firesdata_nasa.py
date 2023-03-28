from datetime import datetime
import pendulum
from docker.types import Mount
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.providers.docker.operators.docker import DockerOperator

locals_tz = pendulum.timezone("America/Bogota")

with DAG(
    dag_id="firesdata_pipeline",
    description="data pipeline for fires data NASA",
    start_date=datetime(year=2023, month=3, day=28, tzinfo=locals_tz),
    schedule="@daily",
    catchup=False,
    tags=["dev"]
):

    with TaskGroup(group_id="fetch_data_nasa") as fetch_data_nasa:
        """Fetch data from NASA website"""
        MODIS_FILENAME = "MODIS-data.csv"
        VIIRS_FILENAME = "VIIRS-data.csv"

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

    chain([fetch_modis_data, fetch_viirs_data])
