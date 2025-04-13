# from flask import Flask, render_template, request, jsonify, send_from_directory
# from pymongo import MongoClient
# import os
# from etl_pipeline import run_etl

# app = Flask(__name__)

# # Directory to save output CSV files
# OUTPUT_FOLDER = "output_csv"
# if not os.path.exists(OUTPUT_FOLDER):
#     os.makedirs(OUTPUT_FOLDER)

# @app.route("/")
# def index():
#     return render_template("index.html")

# @app.route("/fetch_collections", methods=["POST"])
# def fetch_collections():
#     try:
#         mongo_uri = request.json.get("mongo_uri")
#         if not mongo_uri:
#             raise ValueError("MongoDB URI is missing")

#         client = MongoClient(mongo_uri)
#         db_name = mongo_uri.split("/")[-1].split("?")[0]
#         db = client[db_name]

#         collections = db.list_collection_names()
#         return jsonify({"collections": collections})
#     except Exception as e:
#         print(f"Error fetching collections: {e}")
#         return jsonify({"error": str(e)}), 400

# @app.route("/run_etl", methods=["POST"])
# def run_etl_pipeline():
#     try:
#         mongo_uri = request.json.get("mongo_uri")
#         collections = request.json.get("collections")
#         if not collections:
#             raise ValueError("No collections selected")

#         # Run ETL pipeline
#         run_etl(mongo_uri, collections, OUTPUT_FOLDER)

#         # List generated CSV files
#         csv_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.endswith(".csv")]
#         return jsonify({"csv_files": csv_files})
#     except Exception as e:
#         print(f"Error running ETL pipeline: {e}")
#         return jsonify({"error": str(e)}), 400

# @app.route("/download/<filename>")
# def download_file(filename):
#     return send_from_directory(OUTPUT_FOLDER, filename, as_attachment=True)

# if __name__ == "__main__":
#     app.run(debug=True)


from flask import Flask, render_template, request, jsonify, send_from_directory
from pymongo import MongoClient
import os
from etl_pipeline import run_etl

app = Flask(__name__)

# Directory to save output CSV files
OUTPUT_FOLDER = "output_csv"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run_etl", methods=["POST"])
def run_etl_pipeline():
    try:
        mongo_uri = request.json.get("mongo_uri")
        if not mongo_uri:
            raise ValueError("MongoDB URI is missing")

        client = MongoClient(mongo_uri)
        db_name = mongo_uri.split("/")[-1].split("?")[0]
        db = client[db_name]

        # Automatically fetch all collections
        collections = db.list_collection_names()
        if not collections:
            raise ValueError("No collections found in the database")

        # Run ETL pipeline
        run_etl(mongo_uri, collections, OUTPUT_FOLDER)

        # List generated CSV files
        csv_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.endswith(".csv")]
        return jsonify({"csv_files": csv_files})
    except Exception as e:
        print(f"Error running ETL pipeline: {e}")
        return jsonify({"error": str(e)}), 400

@app.route("/download/<filename>")
def download_file(filename):
    return send_from_directory(OUTPUT_FOLDER, filename, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
