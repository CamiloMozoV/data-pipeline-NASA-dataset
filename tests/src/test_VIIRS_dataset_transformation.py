from chispa.dataframe_comparer import assert_df_equality
from src.VIIRS_dataset_transformation import (
    transform_columns_todatetime
)

def test_transform_columns_todatetime(mock_raw_data_viirs,
                                      mock_datetime_transf_data_viirs):
    output_df = transform_columns_todatetime(mock_raw_data_viirs)
    expected_df = mock_datetime_transf_data_viirs

    columns_expected_df = ["latitude", "longitude", "bright_ti4", "scan",
                           "track", "satellite", "confidence", "version", 
                           "bright_ti5", "frp", "daynight", "acq_datetime"]
    
    assert output_df.columns == columns_expected_df
    assert_df_equality(output_df, expected_df)