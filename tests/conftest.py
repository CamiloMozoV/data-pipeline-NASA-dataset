import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark_session = (SparkSession.builder
                                 .master("spark://172.28.0.4:7077")
                                 .appName("Test")
                                 .getOrCreate()
                    )
    yield spark_session
    spark_session.stop()
                                