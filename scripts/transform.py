import pandas as pd


class Transform:
    def __init__(self, taxi_data, payment_data, vendor_data):
        """
        Initialize the Transform class with taxi, payment, and vendor data.

        Args:
        - taxi_data (pd.DataFrame): DataFrame containing taxi trip data.
        - payment_data (pd.DataFrame): DataFrame containing payment lookup data.
        - vendor_data (pd.DataFrame): DataFrame containing vendor lookup data.
        """
        self.taxi_data = taxi_data
        self.payment_data = payment_data
        self.vendor_data = vendor_data

    def calculate_most_trips_per_year(self):
        """
        Calculate the number of trips per vendor per year.

        Returns:
        - pd.DataFrame: DataFrame with columns ['vendor_id', 'year', 'trip_count'].
        """
        df = self.taxi_data.copy()
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['year'] = df['pickup_datetime'].dt.year
        trips_per_year = df.groupby(['vendor_id', 'year']).size().reset_index(name='trip_count')
        return trips_per_year

    def calculate_most_trips_per_week(self):
        """
        Calculate the number of trips per week.

        Returns:
        - pd.DataFrame: DataFrame with columns ['year', 'week', 'trip_count'].
        """
        df = self.taxi_data.copy()
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['year'] = df['pickup_datetime'].dt.year
        df['week'] = df['pickup_datetime'].dt.isocalendar().week
        most_trips_per_week = df.groupby(['year', 'week']).size().reset_index(name='trip_count')
        return most_trips_per_week

    def calculate_vendor_with_most_trips_per_year(self):
        """
        Calculate the vendor with the most trips per year.

        Returns:
        - pd.DataFrame: DataFrame with columns ['vendor_id', 'pickup_datetime', 'total_amount'].
        """
        df = self.taxi_data.copy()
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
        df = df.groupby(['vendor_id', 'pickup_datetime']).agg({'total_amount': 'sum'}).reset_index()
        max_trips_per_vendor = df.sort_values(by=['vendor_id', 'total_amount'], ascending=[True, False]) \
            .drop_duplicates(subset=['vendor_id'], keep='first')
        return max_trips_per_vendor

    def process_data(self):
        """
        Process the taxi data and return transformed data.

        Returns:
        - pd.DataFrame: Transformed DataFrame based on calculations.
        """
        most_trips_year = self.calculate_most_trips_per_year()
        most_trips_week = self.calculate_most_trips_per_week()
        most_trips_vendor_year = self.calculate_vendor_with_most_trips_per_year()

        # Combine or manipulate data as needed
        transformed_data = pd.merge(most_trips_year, most_trips_week, on=['year'], how='outer')
        transformed_data = pd.merge(transformed_data, most_trips_vendor_year, on=['vendor_id'], how='outer')

        return transformed_data
