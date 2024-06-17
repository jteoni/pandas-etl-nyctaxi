import os
from pathlib import Path
from scripts.extract import Extractor
from scripts.transform import Transformer
from scripts.load import Loader


def main():
    data_dir = "data/"
    output_dir = "output/"

    # Create the output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Initialize the Extractor to load data from the 'data/' directory
    extractor = Extractor(data_dir)

    # Load specific data files
    taxi_data = extractor.load_taxi_data()
    payment_data = extractor.load_payment_lookup()
    vendor_data = extractor.load_vendor_lookup()

    # Initialize the Transformer with the loaded data
    transformer = Transformer(taxi_data, payment_data, vendor_data)

    # Process the loaded data to transform it
    transformed_data = transformer.process_data()

    # Initialize the Loader to save the transformed data to 'output/etl_output.csv'
    loader = Loader(output_dir)

    # Save the transformed data to a CSV file
    loader.save_data(transformed_data, filename='etl_output.csv')


if __name__ == '__main__':
    # Execute the main function if the script is run directly
    main()
