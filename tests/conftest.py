import pytest
import os
import sys
from pathlib import Path
import pandas as pd

# Get the root directory of the project
root_dir = str(Path(__file__).resolve().parent.parent)
# Add the root directory to the system path to import local modules
sys.path.insert(0, root_dir)

# Import required modules from local scripts
from scripts.extract import Extract
from scripts.transform import Transform
from scripts.load import Load


@pytest.fixture(scope="session")
def sample_data():
    return {
        'taxi_data': pd.DataFrame({
            'vendor_id': ['CMT', 'VTS', 'CMT', 'DDS'],
            'trip_distance': [5.4, 6.2, 7.1, 8.5]
        }),
        'payment_data': pd.DataFrame({
            'payment_type': ['Cash', 'Credit', 'Cash'],
            'payment_lookup': ['Cash', 'Credit', 'Cash']
        }),
        'vendor_data': pd.DataFrame({
            'vendor_id': ['CMT', 'VTS', 'DDS'],
            'name': ['Creative Mobile', 'VeriFone', 'Dependable Driver']
        })
    }


@pytest.fixture(scope="session")
def extractor():
    data_dir = "data"
    return Extract(data_dir)


@pytest.fixture(scope="session")
def transformer(sample_data):
    taxi_data = sample_data['taxi_data']
    payment_data = sample_data['payment_data']
    vendor_data = sample_data['vendor_data']
    return Transform(taxi_data, payment_data, vendor_data)


@pytest.fixture(scope="session")
def loader():
    output_dir = "output"
    # Create output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return Load(output_dir)


@pytest.fixture(scope="session")
def data_dir():
    return "data"


@pytest.fixture(scope="session")
def output_dir():
    return "output"


def test_load_taxi_data(extractor):
    # Test loading taxi data
    data = extractor.load_taxi_data()
    assert data is not None
    assert len(data) > 0


def test_load_payment_lookup(extractor):
    # Test loading payment lookup data
    data = extractor.load_payment_lookup()
    assert data is not None
    assert len(data) > 0
    assert "payment_type" in data.columns
    assert "payment_lookup" in data.columns


def test_load_vendor_lookup(extractor):
    # Test loading vendor lookup data
    data = extractor.load_vendor_lookup()
    assert data is not None
    assert len(data) > 0
    assert "vendor_id" in data.columns
    assert "name" in data.columns


def test_save_data(loader):
    # Test saving data
    data = pd.DataFrame({
        'year': [2009, 2010],
        'vendor': ['CMT', 'VTS'],
        'most_trips_total': [12000, 13000],
        'most_trips_week': [23, 17],
        'most_trips_week_count': [500, 550],
        'most_trips_vendor_week_count': [300, 320]
    })
    loader.save_data(data)

    output_file = os.path.join("output", 'etl_output.csv')
    # Assert that the output CSV file exists
    assert os.path.isfile(output_file)

    # Assert that saved data matches the original data
    saved_data = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(data, saved_data)
