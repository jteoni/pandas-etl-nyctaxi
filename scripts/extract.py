import os
import pandas as pd


class Extract:
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def load_taxi_data(self):
        try:
            # Find all JSON files in the specified directory
            json_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
            if not json_files:
                raise FileNotFoundError(f"No JSON files found in directory: {self.data_dir}")

            data_frames = []
            # Iterate through each JSON file
            for file in json_files:
                with open(os.path.join(self.data_dir, file), 'r') as f:
                    # Read the entire file content as JSON
                    json_data = f.read()
                    # Convert JSON data into a Pandas DataFrame
                    data = pd.read_json(json_data, lines=True)
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
        try:
            payment_lookup_path = os.path.join(self.data_dir, 'data-payment_lookup.csv')
            if not os.path.isfile(payment_lookup_path):
                raise FileNotFoundError(f"File not found: {payment_lookup_path}")
            # Load payment lookup data from CSV file
            return pd.read_csv(payment_lookup_path)
        except FileNotFoundError as e:
            print(f"Error loading payment lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading payment lookup data: {e}")
            raise

    def load_vendor_lookup(self):
        try:
            vendor_lookup_path = os.path.join(self.data_dir, 'data-vendor_lookup.csv')
            if not os.path.isfile(vendor_lookup_path):
                raise FileNotFoundError(f"File not found: {vendor_lookup_path}")
            # Load vendor lookup data from CSV file
            return pd.read_csv(vendor_lookup_path)
        except FileNotFoundError as e:
            print(f"Error loading vendor lookup file: {e}")
            raise
        except Exception as e:
            print(f"Unexpected error loading vendor lookup data: {e}")
            raise
