from pymongo import MongoClient
import pandas as pd
import os

def clean_data(dataframe):
    """Clean the data by replacing all missing or NaN values with 'NON'."""
    # Replace all missing values in the dataframe with "NON"
    dataframe = dataframe.fillna("NON")
    return dataframe

def transform_data(dataframe, collection_name):
    """Apply any specific transformations based on the collection."""
    if collection_name == "orders":
        # Ensure order_date is properly formatted; replace invalid with 'NON'
        if 'order_date' in dataframe.columns:
            dataframe['order_date'] = pd.to_datetime(dataframe['order_date'], errors='coerce').fillna("NON")

    if collection_name == "products":
        # Ensure price is properly formatted; replace invalid with 'NON'
        if 'price' in dataframe.columns:
            dataframe['price'] = pd.to_numeric(dataframe['price'], errors='coerce').fillna("NON")

    return dataframe

def run_etl(mongo_uri, db_name, collection_names, output_folder="output_csv"):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]

    # Initialize a dictionary to store dataframes for all collections
    dataframes = {}

    # Extract data from each collection
    for collection in collection_names:
        raw_data = list(db[collection].find())
        dataframe = pd.DataFrame(raw_data)
        print(f"Extracted {len(dataframe)} records from {collection} collection.")

        if not dataframe.empty:
            # Clean the data while keeping all columns
            dataframe = clean_data(dataframe)

            # Transform the data
            dataframe = transform_data(dataframe, collection)

            # Add processed timestamp
            dataframe['processed_at'] = pd.Timestamp.now()

            # Save to CSV
            csv_file = os.path.join(output_folder, f"{collection}.csv")
            dataframe.to_csv(csv_file, index=False)
            print(f"Processed and saved {len(dataframe)} records to {csv_file}.")

        else:
            print(f"No data to process for {collection} collection.")
