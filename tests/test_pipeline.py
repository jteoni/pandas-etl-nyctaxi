import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import pytest
import pandas as pd
from scripts.extract import Extractor
from scripts.transform import Transformer
from scripts.load import Loader


@pytest.fixture
def data_dir():
    return 'data'  # Assuming 'data' directory contains the necessary files for extraction


@pytest.fixture
def output_dir():
    return 'output'  # Output directory for saving test data


@pytest.fixture
def extractor(data_dir):
    return Extractor(data_dir)


@pytest.fixture
def transformer():
    # Mocking data for Transformer initialization (taxi_data, payment_data, vendor_data)
    taxi_data = pd.DataFrame({
        'pickup_datetime': pd.to_datetime(['2023-01-01 10:00', '2023-01-02 11:00']),
        'vendor_id': ['CMT', 'VTS'],
        'total_amount': [10.0, 15.0]
    })
    payment_data = pd.DataFrame({
        'payment_type': ['Cash', 'Credit'],
        'payment_lookup': ['Cash', 'Credit']
    })
    vendor_data = pd.DataFrame({
        'vendor_id': ['CMT', 'VTS'],
        'name': ['Creative Mobile', 'VeriFone']
    })
    return Transformer(taxi_data, payment_data, vendor_data)


@pytest.fixture
def loader(output_dir):
    return Loader(output_dir)


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'vendor_id': ['CMT', 'VTS'],
        'year': [2023, 2023],
        'trip_count': [1, 1],
        'week': [1, 1],
        'pickup_datetime': ['2023-01-01 10:00', '2023-01-02 11:00'],
        'total_amount': [10.0, 15.0]
    })


@pytest.mark.usefixtures("data_dir", "output_dir", "extractor", "transformer", "loader", "sample_data")
class TestPipeline:

    def test_extract_data(self, extractor):
        # Test data extraction
        data = extractor.load_taxi_data()
        assert data is not None
        assert len(data) > 0
        assert isinstance(data, pd.DataFrame)

    def test_transform_data(self, transformer):
        # Test data transformation
        transformed_data = transformer.process_data()

        expected_columns = {'vendor_id', 'year', 'trip_count_y', 'week', 'pickup_datetime', 'total_amount'}
        assert transformed_data is not None
        assert len(transformed_data) > 0
        assert set(transformed_data.columns) == expected_columns

    def test_load_data(self, loader, sample_data):
        # Test data loading
        loader.save_data(sample_data)
        output_file = os.path.join('output', 'output.csv')
        assert os.path.isfile(output_file)

        # Verify the saved content
        saved_data = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(sample_data, saved_data)
