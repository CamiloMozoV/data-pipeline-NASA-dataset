import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from utils import read_csv_data_from_s3, save_csv_data_to_s3

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

def transform_confid_percent_to_confid_level(df: DataFrame) -> DataFrame:
    """Transform the confidence field 'confidence', which is an integer 
    value, into a textual explanation as expressed in the VIIRS dataset.

    - if the confidence is less than or equal to 40%, the level is low.
    - if the confidence is more than 40%, but below 100%, then the level is nominal.
    - Any other value (100%) id high

    Parameters
    ----------
    `df`: `pyspark.sql.DataFrame`

    Return
    ------
    `df`: `pyspark.sql.DataFrame`
    """
    low_confidence = 40
    high_confidence = 100
    return (df.withColumn("confidence_level", F.when(F.col("confidence") <= F.lit(low_confidence), "low")
                        .when((F.col("confidence") > F.lit(low_confidence)) & (F.col("confidence") < F.lit(high_confidence)), "nominal")
                        .when(F.isnull(F.col("confidence")), "high")
                        .otherwise("high"))
             .drop("confidence")
            )

def modis_data_transformation(df: DataFrame) -> DataFrame:
    """Perform data transformation to MODIS dataset.

    Parameter
    ---------
    `df`: `pyspark.sql.DataFrame`

    Return
    -------
    `pyspark.sql.DataFrame`
    """
    return (df.transform(transform_columns_todatetime)              # transf columns to datetime
             .transform(transform_confid_percent_to_confid_level)   # transf confidence columns 
             .withColumn("bright_ti4", F.lit(None).cast("string"))
             .withColumn("bright_ti5", F.lit(None).cast("string"))      
            )

def main(spark_session: SparkSession) -> None:

    BUCKET_NAME = "transition-storage-dev"
    MODIS_FILENAME = "MODIS-data"

    raw_df = read_csv_data_from_s3(spark_session, 
                                   bucket_name=BUCKET_NAME, 
                                   s3_key="raw-data", 
                                   filename=f"{MODIS_FILENAME}.csv")
    
    # MODIS data transformation
    clean_df = modis_data_transformation(raw_df)

    # Uploading Data
    save_csv_data_to_s3(clean_df, 
                 bucket_name=BUCKET_NAME,
                 s3_key="clean-data",
                 filename=f"clean-{MODIS_FILENAME}"
                )


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

