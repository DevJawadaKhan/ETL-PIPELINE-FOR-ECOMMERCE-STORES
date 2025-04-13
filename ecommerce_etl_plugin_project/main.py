# main.py

from ecommerce_etl_plugin.plugin import ETLPlugin

def main():
    mongo_uri = "mongodb+srv://chtalhaarain:090078601@cluster0.8yhs7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    db_name = "test"
    collection_names = ["users", "categories", "orders", "products"]
    output_folder = "output_csv"  # Folder where CSV files will be saved

    plugin = ETLPlugin(mongo_uri, db_name, collection_names, output_folder)
    result = plugin.run_pipeline()
    print(result)

if __name__ == "__main__":
    main()
