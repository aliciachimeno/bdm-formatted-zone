import os
import pandas as pd
import pickle
import zipfile
from pyspark.sql import SparkSession

from bson.binary import Binary

from data_formatter.data_loader import DataLoader
from data_formatter.data_formatter import DataFormatter
from utils.mongo import MongoDBUtils

from paths import descriptive_data, predictive_data

DB_NAME = 'tables'
DB_EXPLOITATION = 'exploitation'

def main():
    
    try:
        # Creation of the spark session
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("FormmatedZone") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .config('spark.mongodb.input.uri', f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{DB_NAME}?retryWrites=true&w=majority&appName=BDM") \
            .config('spark.mongodb.output.uri', f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{DB_NAME}?retryWrites=true&w=majority&appName=BDM") \
            .config('spark.mongodb.input.uri', f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{DB_EXPLOITATION}?retryWrites=true&w=majority&appName=BDM") \
            .config('spark.mongodb.output.uri', f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{DB_EXPLOITATION}?retryWrites=true&w=majority&appName=BDM") \
            .getOrCreate()
    except Exception as e:
        print(f"Error on the Spark Session creation: {e}")
    
    ########################### DATA FORMATTING ##################################
    
    dataL = DataLoader(spark)
    dfs = dataL.dfs  # Extract the dfs dictionary
    dataF = DataFormatter(dfs)
    
    mongo_formatted = MongoDBUtils(DB_NAME)
    for name, df in dataF.dfs.items():
        mongo_formatted.write_to_collection(name, df, append=False)
        
    ###################### UPLOAD EXPLOITATION TO MONGO ############################
    
    # As specified in the report the predictive and descriptive analysis have been done locally in Jupyter Notebooks
    # so here the upload of the final data frames and model used in the creation of that and the KPIs is done in order
    # to maintain our data distributed in the same format so if in further stages of the process we wanted to change
    # our model or our KPIs we would have access to the data stored in Mongo.
    
    # Upload the data we had from our notebooks
    def upload_csv_to_mongo(mongo, file_path, collection_name):
        df = pd.read_csv(file_path)
        mongo.write_to_collection_df(collection_name, df, append=False)
        
    def zip_directory(directory_path, zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    zip_file.write(os.path.join(root, file),
                                os.path.relpath(os.path.join(root, file),
                                                os.path.join(directory_path, '..')))
        
    def upload_model_to_mongo(mongo_utils, model_dir_path, collection_name):
        zip_file_path = f"{model_dir_path}.zip"
        zip_directory(model_dir_path, zip_file_path)

        with open(zip_file_path, 'rb') as f:
            serialized_model = Binary(f.read())

        mongo_utils.write_to_collection_df(collection_name, {'model': serialized_model}, append=False)
        os.remove(zip_file_path)  # Clean up the zip file after upload
    
    ################################################################################
    
    mongo_exploitation = MongoDBUtils(DB_EXPLOITATION)
    
    csvs = [(descriptive_data+'/kpi1.csv', 'kpi1'),
            (descriptive_data+'/kpi2.csv', 'kpi2'),
            (descriptive_data+'/kpi3.csv', 'kpi3'),
            (predictive_data+'/model_data.csv', 'predictive_data')
            ]
    
    model_dir_path = predictive_data+'/linear_regression'
    
    for file_path, collection_name in csvs:
        upload_csv_to_mongo(mongo_exploitation, file_path, collection_name)
    
    upload_model_to_mongo(mongo_exploitation, model_dir_path, 'predictive_model')
    
    ################################################################################
    
if __name__ == "__main__":
    main()
