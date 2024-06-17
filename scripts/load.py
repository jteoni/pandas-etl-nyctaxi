import pandas as pd
import os


class Loader:
    def __init__(self, output_path):
        """
        Initialize the Loader class with an output path.

        Args:
        - output_path (str): The path where data will be saved.
        """
        self.output_path = output_path

    def save_data(self, data, filename='output.csv'):
        """
        Save data to a CSV file at the specified output path.

        Args:
        - data (pd.DataFrame): The DataFrame to be saved.
        - filename (str, optional): Name of the output CSV file. Default is 'output.csv'.

        Raises:
        - TypeError: If data is not a Pandas DataFrame.
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Expected a pandas DataFrame")

        try:
            output_file = os.path.join(self.output_path, filename)
            # Save the DataFrame to a CSV file without including the index
            data.to_csv(output_file, index=False)
            print(f"Data saved to {output_file}")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")
            raise

    def _get_output_file_path(self, filename):
        """
        Generate the full path to the output file.

        Args:
        - filename (str): Name of the output CSV file.

        Returns:
        - str: Full path to the output CSV file.
        """
        return f"{self.output_path}/{filename}"
