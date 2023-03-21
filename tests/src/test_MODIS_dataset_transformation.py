from chispa.dataframe_comparer import assert_df_equality
from src.MODIS_dataset_transformation import transform_columns_todatetime

def test_transform_columns_todatetime(spark_session, 
                                      mock_raw_data_modis,
                                      mock_datetime_transf_data_modis):
    
    output_df = transform_columns_todatetime(mock_raw_data_modis)
    expected_df = mock_datetime_transf_data_modis
    
    columns_expected_df = ["latitude", "longitude", "brightness", "scan",
                           "track", "satellite", "confidence", "version", 
                           "bright_t31", "frp", "daynight", "acq_datetime"]

    assert_df_equality(output_df, expected_df)
    assert output_df.columns == columns_expected_df

