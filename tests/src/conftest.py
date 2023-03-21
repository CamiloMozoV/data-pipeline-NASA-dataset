from datetime import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

@pytest.fixture(scope="session")
def mock_raw_data_modis(spark_session: SparkSession):
    
    raw_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("brightness", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("acq_date", T.TimestampType(), True),
        T.StructField("acq_time", T.IntegerType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("confidence", T.IntegerType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_t31", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True),
    ])

    raw_df = spark_session.createDataFrame(
        data=[
            (-20.10942, 148.14326, 314.39, 1.0, 1.0, datetime(2023,3,15,0,0,0), 5, "T", 20, "6.1NRT", 296.82, 8.8, "D"),
            (-23.21878, 148.91298, 314.08, 1.04, 1.02, datetime(2023,3,15,0,0,0), 5, "T", 58, "6.1NRT", 298.73, 7.4, "D"),
            (-24.4318, 151.83102, 307.98, 1.48, 1.2, datetime(2023,3,15,0,0,0), 5, "T", 50, "6.1NRT", 292.72, 8.79, "D"),
            (-25.70059, 149.48932, 313.9, 1.14, 1.06, datetime(2023,3,15,0,0,0), 5, "T", 51, "6.1NRT", 294.55, 5.15, "D"),
            (-26.7598, 147.14514, 361.54, 1.0, 1.0, datetime(2023,3,15,0,0,0), 5, "T", 100, "6.1NRT", 306.81, 79.4, "D")
        ],
        schema=raw_schema
    )
    
    return raw_df

@pytest.fixture(scope="session")
def mock_datetime_transf_data_modis(spark_session: SparkSession):

    datetime_transf_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("brightness", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("confidence", T.IntegerType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_t31", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True),
        T.StructField("acq_datetime", T.StringType(), True),
    ])

    datetime_transf_df = spark_session.createDataFrame(
        data=[
          (-20.10942, 148.14326, 314.39, 1.0, 1.0, "T", 20, "6.1NRT", 296.82, 8.8, "D", "2023-03-15 00:05:00"),
          (-23.21878, 148.91298, 314.08, 1.04, 1.02, "T", 58, "6.1NRT", 298.73 , 7.4, "D", "2023-03-15 00:05:00"),
          (-24.4318, 151.83102, 307.98, 1.48, 1.2, "T", 50, "6.1NRT", 292.72, 8.79, "D", "2023-03-15 00:05:00"),
          (-25.70059, 149.48932, 313.9, 1.14, 1.06, "T", 51, "6.1NRT", 294.55, 5.15, "D", "2023-03-15 00:05:00"),
          (-26.7598, 147.14514, 361.54, 1.0, 1.0, "T", 100, "6.1NRT", 306.81, 79.4, "D", "2023-03-15 00:05:00")
        ],
        schema=datetime_transf_schema
    )
    return datetime_transf_df

@pytest.fixture(scope="session")
def mock_confidence_transf_data_modis(spark_session: SparkSession):

    confidence_transf_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("brightness", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("acq_date", T.TimestampType(), True),
        T.StructField("acq_time", T.IntegerType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_t31", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True),
        T.StructField("confidence_level", T.StringType(), False),
    ])

    confidence_transf_df = spark_session.createDataFrame(
        data=[
            (-20.10942, 148.14326, 314.39, 1.0, 1.0, datetime(2023,3,15,0,0,0), 5, "T", "6.1NRT", 296.82, 8.8, "D", "low"),
            (-23.21878, 148.91298, 314.08, 1.04, 1.02, datetime(2023,3,15,0,0,0), 5, "T", "6.1NRT", 298.73, 7.4, "D", "nominal"),
            (-24.4318, 151.83102, 307.98, 1.48, 1.2, datetime(2023,3,15,0,0,0), 5, "T", "6.1NRT", 292.72, 8.79, "D", "nominal"),
            (-25.70059, 149.48932, 313.9, 1.14, 1.06, datetime(2023,3,15,0,0,0), 5, "T", "6.1NRT", 294.55, 5.15, "D", "nominal"),
            (-26.7598, 147.14514, 361.54, 1.0, 1.0, datetime(2023,3,15,0,0,0), 5, "T", "6.1NRT", 306.81, 79.4, "D", "high")
        ],
        schema=confidence_transf_schema
    )
    return confidence_transf_df

@pytest.fixture(scope="session")
def mock_modis_data_transformation(spark_session: SparkSession):

    data_transf_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("brightness", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_t31", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True),
        T.StructField("acq_datetime", T.StringType(), True),
        T.StructField("confidence_level", T.StringType(), False),
        T.StructField("bright_ti4", T.StringType(), True),
        T.StructField("bright_ti5", T.StringType(), True)
    ])

    data_transf_df = spark_session.createDataFrame(
        data=[
            (-20.10942, 148.14326, 314.39, 1.0, 1.0, "T", "6.1NRT", 296.82, 8.8, "D", "2023-03-15 00:05:00", "low", None, None),      
            (-23.21878, 148.91298, 314.08, 1.04, 1.02,"T", "6.1NRT", 298.73, 7.4, "D", "2023-03-15 00:05:00", "nominal", None, None),      
            (-24.4318, 151.83102, 307.98, 1.48, 1.2, "T", "6.1NRT", 292.72, 8.79, "D", "2023-03-15 00:05:00", "nominal", None, None),      
            (-25.70059, 149.48932, 313.9, 1.14, 1.06, "T", "6.1NRT", 294.55, 5.15, "D", "2023-03-15 00:05:00", "nominal", None, None),      
            (-26.7598, 147.14514, 361.54, 1.0, 1.0, "T", "6.1NRT", 306.81, 79.4, "D", "2023-03-15 00:05:00", "high", None, None)
        ],
        schema=data_transf_schema
    )
    return data_transf_df

@pytest.fixture(scope="session")
def mock_raw_data_viirs(spark_session: SparkSession):

    raw_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("bright_ti4", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("acq_date", T.TimestampType(), True),
        T.StructField("acq_time", T.IntegerType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("confidence", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_ti5", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True)
    ])

    raw_df = spark_session.createDataFrame(
        data=[
            (6.60031, 11.09895, 297.75, 0.44, 0.39, datetime(2023,3,15,0,0,0), 35, "N", "nominal", "2.0NRT", 286.45, 0.23, "N"),
            (5.93611, 13.53046, 295.41, 0.38, 0.36, datetime(2023,3,15,0,0,0), 35, "N", "nominal", "2.0NRT", 276.39, 0.27, "N"),
            (5.63077, 13.72671, 305.08, 0.38, 0.36, datetime(2023,3,15,0,0,0), 35, "N", "nominal", "2.0NRT", 270.5, 0.88, "N"),
            (5.56914, 13.73055, 308.48, 0.38, 0.36, datetime(2023,3,15,0,0,0), 35, "N", "nominal", "2.0NRT", 267.92, 1.17, "N"),
            (5.56415, 13.71915, 307.02, 0.38, 0.36, datetime(2023,3,15,0,0,0), 35, "N", "nominal", "2.0NRT", 269.61, 1.2, "N"),
        ],
        schema=raw_schema
    )
    return raw_df

@pytest.fixture(scope="session")
def mock_datetime_transf_data_viirs(spark_session: SparkSession):
    datetime_transf_schema = T.StructType([
        T.StructField("latitude", T.DoubleType(), True),
        T.StructField("longitude", T.DoubleType(), True),
        T.StructField("bright_ti4", T.DoubleType(), True),
        T.StructField("scan", T.DoubleType(), True),
        T.StructField("track", T.DoubleType(), True),
        T.StructField("satellite", T.StringType(), True),
        T.StructField("confidence", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("bright_ti5", T.DoubleType(), True),
        T.StructField("frp", T.DoubleType(), True),
        T.StructField("daynight", T.StringType(), True),
        T.StructField("acq_datetime", T.StringType(), True)
    ])

    datetime_transf_df = spark_session.createDataFrame(
        data=[
            (6.60031, 11.09895, 297.75, 0.44, 0.39, "N", "nominal", "2.0NRT", 286.45, 0.23, "N", "2023-03-15 00:35:00"),
            (5.93611, 13.53046, 295.41, 0.38, 0.36, "N", "nominal", "2.0NRT", 276.39, 0.27, "N", "2023-03-15 00:35:00"),
            (5.63077, 13.72671, 305.08, 0.38, 0.36, "N", "nominal", "2.0NRT", 270.5, 0.88, "N", "2023-03-15 00:35:00"),
            (5.56914, 13.73055, 308.48, 0.38, 0.36, "N", "nominal", "2.0NRT", 267.92, 1.17, "N", "2023-03-15 00:35:00"),
            (5.56415, 13.71915, 307.02, 0.38, 0.36, "N", "nominal", "2.0NRT", 269.61, 1.2, "N", "2023-03-15 00:35:00"),
        ],
        schema=datetime_transf_schema
    )

    return datetime_transf_df