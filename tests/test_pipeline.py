import pytest
import os
import pandas as pd


# Utilize pytest's mark to specify fixtures to be used in the test class
@pytest.mark.usefixtures("data_dir", "output_dir", "extractor", "transformer", "loader", "sample_data")
class TestPipeline:

    def test_extract_data(self, data_dir, extractor):
        # Test data extraction
        data = extractor.load_taxi_data()
        assert data is not None
        assert len(data) > 0

    def test_transform_data(self, sample_data, transformer):
        # Test data transformation
        transformed_data = transformer.transform_vendors(sample_data)
        assert transformed_data is not None
        assert len(transformed_data) == len(sample_data)
        assert "vendor_id" in transformed_data.columns
        assert "vendor_name" in transformed_data.columns

    def test_load_data(self, output_dir, loader, sample_data):
        # Test data loading
        loader.save_data(sample_data)
        output_file = os.path.join(output_dir, 'etl_output.csv')
        assert os.path.isfile(output_file)

        # Verify the saved content
        saved_data = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(sample_data, saved_data)
