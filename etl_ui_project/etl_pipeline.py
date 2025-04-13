# from pymongo import MongoClient
# import pandas as pd
# import os

# def run_etl(mongo_uri, collection_names, output_folder):
#     client = MongoClient(mongo_uri)
#     db_name = mongo_uri.split("/")[-1].split("?")[0]
#     db = client[db_name]

#     for collection in collection_names:
#         try:
#             data = list(db[collection].find())
#             df = pd.DataFrame(data)

#             # Replace missing values with "NON"
#             df = df.fillna("NON")

#             # Add processed timestamp
#             df["processed_at"] = pd.Timestamp.now()

#             # Save to CSV
#             csv_path = os.path.join(output_folder, f"{collection}.csv")
#             df.to_csv(csv_path, index=False)
#             print(f"Processed collection '{collection}' and saved to '{csv_path}'")
#         except Exception as e:
#             print(f"Error processing collection '{collection}': {e}")


from pymongo import MongoClient
import pandas as pd
import os

def run_etl(mongo_uri, collection_names, output_folder):
    client = MongoClient(mongo_uri)
    db_name = mongo_uri.split("/")[-1].split("?")[0]
    db = client[db_name]

    for collection in collection_names:
        try:
            data = list(db[collection].find())
            df = pd.DataFrame(data)

            # Replace missing values with "NON"
            df = df.fillna("NON")

            # Add processed timestamp
            df["processed_at"] = pd.Timestamp.now()

            # Save to CSV
            csv_path = os.path.join(output_folder, f"{collection}.csv")
            df.to_csv(csv_path, index=False)
            print(f"Processed collection '{collection}' and saved to '{csv_path}'")
        except Exception as e:
            print(f"Error processing collection '{collection}': {e}")
