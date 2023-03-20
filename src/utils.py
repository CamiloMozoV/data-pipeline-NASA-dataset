from pyspark.sql import SparkSession, DataFrame

def read_data_from_s3(spark_session: SparkSession, 
                      bucket_name: str, 
                      s3_key: str, 
                      filename: str) -> DataFrame:
    """Read the data from S3 bucket.
    
    Parameters
    -----------
    `spark_session` : `pyspark.sql.SparkSession`
    `bucket_name`: (`str`) the name of the bucket to download from.
    `s3_key`: (`str`) the name of the key [path inside of the bucket]
              to download to.
    `filename` : (`str`) the name of the file to download to.

    Return
    ------
    `pyspark.sql.DataFrame`
    """
    return spark_session.read.csv(path=f"s3a://{bucket_name}/{s3_key}/{filename}",
                                  header=True,
                                  inferSchema=True,
                                  sep=','
                                )

def save_data_s3(df: DataFrame,
                 bucket_name: str,
                 s3_key: str, 
                 filename: str) -> None:
    """Upload clean data into an S3 bucket.
    
    Parameters
    ----------
    `spark_session`: `pyspark.sql.SparkSession`
    `df`: `pyspark.sql.DataFrame` the Dataframe
    `bucket_name`: (`str`) the name of the bucket to upload to.
    `s3_key`: (`str`) the name of the key [path inside of the bucket]
              to upload.
    `filename` : (`str`) the name of the file to upload.
    """
    try:
      (df.write
       .option("header", "true")
       .option('fs.s3a.committer.name', 'file')
       .option('fs.s3a.committer.staging.conflict-mode', 'replace')
       .option("fs.s3a.fast.upload.buffer", "bytebuffer")
       .mode("ignore")
       .csv(path=f"s3a://{bucket_name}/{s3_key}/{filename}",
            sep=',')
      )
    except:
       print("Data cannot be upload.")