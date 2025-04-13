# ecommerce_etl_plugin/plugin.py

class ETLPlugin:
    def __init__(self, mongo_uri, db_name, collection_names, output_folder="output_csv"):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_names = collection_names
        self.output_folder = output_folder

    def run_pipeline(self):
        from .etl_pipeline import run_etl
        try:
            run_etl(self.mongo_uri, self.db_name, self.collection_names, self.output_folder)
            return f"ETL pipeline executed successfully! Files saved to '{self.output_folder}'"
        except Exception as e:
            return f"Error: {str(e)}"
