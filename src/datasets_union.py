import os 
from pyspark.sql import SparkSession
from utils import read_csv_data_from_s3, save_parquet_data_to_s3

def datasets_union(spark_session: SparkSession) -> None:
    """Union (or merge) the two datasets: 
    - MODIS 
    - VIIRS
    for this case.

    Parameter
    ---------
    `spark_session`: `pyspark.sql.SparkSession`
    """

    BUCKET_NAME = "project-bucket-tests"
    S3_KEY = "test-data"
    VIIRS_FILENAME = "VIIRS-data"
    MODIS_FILENAME = "MODIS-data"
    JOIN_DATA = "fires_parquet_dataset"

    viirs_data = read_csv_data_from_s3(spark_session, 
                                       BUCKET_NAME, 
                                       S3_KEY,
                                       f"clean-{VIIRS_FILENAME}")

    modis_data = read_csv_data_from_s3(spark_session,
                                       BUCKET_NAME,
                                       S3_KEY,
                                       f"clean-{MODIS_FILENAME}")

    join_data = viirs_data.unionByName(modis_data, allowMissingColumns=False)
    save_parquet_data_to_s3(join_data, BUCKET_NAME, S3_KEY, JOIN_DATA)

if __name__=="__main__":
    # Create a session
    spark_session = (SparkSession.builder
                     .appName("Join NASA Dataset")
                     .master("spark://spark-master:7077")
                     .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar")
                     .config("spark.jars", "/opt/bitnami/spark/jars/aws-java-sdk-1.12.319.jar")
                     .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-common-3.3.4.jar")
                     .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-client-3.3.4.jar")
                     .config("spark.jars", "/opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.319.jar")
                     .config("spark.sql.adaptive.enable", False)
                     .getOrCreate()
                    )

    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))

    spark_session.sparkContext.setLogLevel("WARN")
    datasets_union(spark_session)
    spark_session.stop()