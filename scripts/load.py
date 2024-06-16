import pandas as pd


class Load:
    def __init__(self, output_path):
        """
        Initialize the Load class with an output path.

        Args:
        - output_path (str): The path where data will be saved.
        """
        self.output_path = output_path

    def save_data(self, data):
        """
        Save data to a CSV file at the specified output path.

        Args:
        - data (pd.DataFrame): The DataFrame to be saved.

        Raises:
        - TypeError: If data is not a Pandas DataFrame.
        """
        if not isinstance(data, pd.DataFrame):
            raise TypeError("Expected a pandas DataFrame")

        try:
            # Save the DataFrame to a CSV file without including the index
            data.to_csv(self.output_path, index=False)
            print(f"Data saved to {self.output_path}")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")
            raise
