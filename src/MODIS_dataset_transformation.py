import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    expr,
    unix_timestamp,
    from_unixtime,
    col,
    lit
)
from src.utils import read_data_from_s3

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
    return df.withColumn("acq_time_min", expr("acq_time % 100"))\
             .withColumn("acq_time_hour", expr("int(acq_time / 100)"))\
             .withColumn("acq_date_unix", unix_timestamp(col("acq_date")))\
             .withColumn("acq_datetime_unix", expr("acq_date_unix + acq_time_min * 60 + acq_time_hour * 3600"))\
             .withColumn("acq_datetime", from_unixtime(col("acq_datetime_unix")))\
             .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hour", "acq_date_unix", "acq_datetime_unix")


def main(spark_session: SparkSession) -> None:
    MODIS_filename = "MODIS-data.csv"
    df = read_data_from_s3(spark_session, filename=MODIS_filename)
    print("\n=====> Raw Data <=====")
    df.printSchema()
    df.sample(0.3).show(10, truncate=False)

    df = transform_columns_todatetime(df)
    print("\n=====> transf columns to datetime <=====")
    df.printSchema()
    df.sample(0.3).show(10, truncate=False)

if __name__=="__main__":
    # Create a session
    spark_session = SparkSession.builder\
                    .appName("Data Nasa Transformation")\
                    .master("spark://spark-master:7077")\
                    .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar")\
                    .config("spark.jars", "/opt/bitnami/spark/jars/aws-java-sdk-1.12.319.jar")\
                    .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-common-3.3.4.jar")\
                    .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-client-3.3.4.jar")\
                    .config("spark.jars", "/opt/bitnami/spark/jars/aws-java-sdk-s3-1.12.319.jar")\
                    .config("spark.sql.adaptive.enable", False)\
                    .getOrCreate()
    
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))

    spark_session.sparkContext.setLogLevel("WARN")
    main(spark_session)
    spark_session.stop()

