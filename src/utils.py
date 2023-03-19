from pyspark.sql import SparkSession, DataFrame

def read_data_from_s3(spark_session: SparkSession, filename: str) -> DataFrame:
    """Read the data from S3 bucket.
    
    Parameters
    -----------
    `spark_session` : `pyspark.sql.SparkSession`
    `filename` : (str) the name of the file to download.

    Return
    ------
    `pyspark.sql.DataFrame`
    """
    return spark_session.read.csv(path=f"s3a://project-bucket-tests/test-data/{filename}",
                                  header=True,
                                  inferSchema=True
                                )