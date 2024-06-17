import os
import pandas as pd
from io import StringIO  # Import StringIO for working with JSON strings


class Extractor:
    def __init__(self, data_dir):
        """
        Initialize the Extractor class with a data directory.

        Args:
        - data_dir (str): Directory path containing data files.
        """
        self.data_dir = data_dir

    def load_taxi_data(self):
        """
        Load taxi trip data from JSON files in the specified directory.

        Returns:
        - pd.DataFrame: Concatenated DataFrame of all JSON files.
        """
        try:
            json_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
            if not json_files:
                raise FileNotFoundError(f"No JSON files found in directory: {self.data_dir}")

            data_frames = []
            for file in json_files:
                with open(os.path.join(self.data_dir, file), 'r') as f:
                    json_data = f.read()
                    # Use StringIO to read JSON string as a buffer
                    json_buffer = StringIO(json_data)
                    data = pd.read_json(json_buffer, lines=True)
                    data_frames.append(data)

            # Concatenate all DataFrames into a single DataFrame
            return pd.concat(data_frames, ignore_index=True)
        except FileNotFoundError as e:
            print(f"Error loading JSON files: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading taxi data: {e}")
            raise

    def load_payment_lookup(self):
        """
        Load payment lookup data from a CSV file in the specified directory.

        Returns:
        - pd.DataFrame: DataFrame containing payment lookup data.
        """
        try:
            payment_lookup_path = os.path.join(self.data_dir, 'data-payment_lookup.csv')
            if not os.path.isfile(payment_lookup_path):
                raise FileNotFoundError(f"File not found: {payment_lookup_path}")

            return pd.read_csv(payment_lookup_path)
        except FileNotFoundError as e:
            print(f"Error loading payment lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading payment lookup data: {e}")
            raise

    def load_vendor_lookup(self):
        """
        Load vendor lookup data from a CSV file in the specified directory.

        Returns:
        - pd.DataFrame: DataFrame containing vendor lookup data.
        """
        try:
            vendor_lookup_path = os.path.join(self.data_dir, 'data-vendor_lookup.csv')
            if not os.path.isfile(vendor_lookup_path):
                raise FileNotFoundError(f"File not found: {vendor_lookup_path}")

            return pd.read_csv(vendor_lookup_path)
        except FileNotFoundError as e:
            print(f"Error loading vendor lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading vendor lookup data: {e}")
            raise
