from chispa.dataframe_comparer import assert_df_equality
from src.MODIS_dataset_transformation import (
    transform_columns_todatetime,
    transform_confid_percent_to_confid_level,
    modis_data_transformation
)

def test_transform_columns_todatetime(mock_raw_data_modis,
                                      mock_datetime_transf_data_modis):
    
    output_df = transform_columns_todatetime(mock_raw_data_modis)
    expected_df = mock_datetime_transf_data_modis
    
    columns_expected_df = ["latitude", "longitude", "brightness", "scan",
                           "track", "satellite", "confidence", "version", 
                           "bright_t31", "frp", "daynight", "acq_datetime"]

    assert_df_equality(output_df, expected_df)
    assert output_df.columns == columns_expected_df

def test_transform_confid_percent_to_confid_level(mock_raw_data_modis,
                                                  mock_confidence_transf_data_modis):
    
    output_df = transform_confid_percent_to_confid_level(mock_raw_data_modis)
    expected_df = mock_confidence_transf_data_modis

    columns_expected_df = ["latitude", "longitude", "brightness", "scan",
                           "track", "acq_date", "acq_time", "satellite", "version", 
                           "bright_t31", "frp", "daynight", "confidence_level"]
    
    assert output_df.columns == columns_expected_df
    assert_df_equality(output_df, expected_df)

def test_modis_data_transformation(mock_raw_data_modis,
                                   mock_modis_data_transformation):
    
    output_df = modis_data_transformation(mock_raw_data_modis)
    expected_df = mock_modis_data_transformation

    columns_expected_df = ["latitude", "longitude", "brightness", "scan",
                           "track", "satellite", "version", "bright_t31", 
                           "frp", "daynight", "acq_datetime",  "confidence_level",
                           "bright_ti4", "bright_ti5"]
    
    assert output_df.columns == columns_expected_df
    assert_df_equality(output_df, expected_df)