import os 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.utils import read_csv_data_from_s3, save_csv_data_to_s3

def transform_columns_todatetime(df: DataFrame) -> DataFrame:
    """Transform the columns corresponding to the date field 'acq_date' 
    (YYYY-mm-dd: ISO standard) and time field 'acq_time' (represented as 
    an integer including only hours and minutes - No seconds) to datetime 
    
    Parameters
    ----------
    `df` : `pyspark.sql.DataFrame`

    Return
    ------
    `df` : `pyspark.sql.DataFrame`
    """
    return (df.withColumn("acq_time_min", F.expr("acq_time % 100"))
             .withColumn("acq_time_hour", F.expr("int(acq_time / 100)"))
             .withColumn("acq_date_unix", F.unix_timestamp("acq_date"))
             .withColumn("acq_datetime_unix", F.expr("acq_date_unix + acq_time_min * 60 + acq_time_hour * 3600"))
             .withColumn("acq_datetime", F.from_unixtime("acq_datetime_unix"))
             .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hour", "acq_date_unix", "acq_datetime_unix")
            )

def viirs_data_transformation(df: DataFrame) -> DataFrame:
    """Perform data transformation to VIIRS dataset.
    
    Parameter
    ---------
    `df`: `pyspark.sql.DataFrame`

    Return
    -------
    `pyspark.sql.DataFrame`
    """
    return (df.transform(transform_columns_todatetime)
            .withColumnRenamed("confidence", "confidence_level")
            .withColumn("brightness", F.lit(None).cast("string"))
            .withColumn("bright_t31", F.lit(None).cast("string"))
        )

def main(spark_session: SparkSession) -> None:

    BUCKET_NAME = "project-bucket-tests"
    S3_KEY = "test-data"
    VIIRS_FILENAME = "VIIRS-data"

    raw_df = read_csv_data_from_s3(spark_session, BUCKET_NAME, S3_KEY, filename=f"{VIIRS_FILENAME}.csv")

    # VIIRS data transformation
    clean_df = viirs_data_transformation(raw_df)

    save_csv_data_to_s3(clean_df, BUCKET_NAME, S3_KEY, filename=f"clean-{VIIRS_FILENAME}")
    

if __name__=="__main__":
    # Create a session
    spark_session = (SparkSession.builder
                    .appName("Data Nasa Transformation")
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
    main(spark_session)
    spark_session.stop()

