from pymongo import MongoClient
from pyspark.sql import SparkSession
import pandas as pd

class MongoDBUtils:
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.client = MongoClient(f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{self.db_name}?retryWrites=true&w=majority&appName=BDM")
        self.db = self.client[self.db_name]
        
    # Function used in the Jupyter Notebooks to retrive the data from the Formatted Zone
    def read_collections_from_mongo(self, spark, collections):
        uri = f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{self.db_name}?retryWrites=true&w=majority&appName=BDM"
        dfs = {}
        for c in collections:
            df = spark.read.format('mongo').option("uri", uri).option('collection', c).option("encoding", "utf-8-sig").load()
        dfs[c] = df

        return dfs
        
    @staticmethod
    def connect(host, port): # host: virtual machine host, port: mongo db port
        return MongoClient(host, port)

    def create_collection(self, collection_name):
        try:
            if collection_name not in self.db.list_collection_names():
                self.db.create_collection(collection_name)
            return True
        except Exception as e:
            print(f"An error occurred during the creation of the collection: {collection_name}. Error: {e}")
            return False

    def drop_collection(self, collection_name):
        try:
            self.db[collection_name].drop()
            return True
        except Exception as e:
            print(f"An error occurred during the dropping of the collection: {collection_name}. Error: {e}")
            return False
        
    def write_to_collection_df(self, collection_name, data, append=True):
        try:
            collection = self.db[collection_name]
            if not append:
                self.drop_collection(collection_name)
                self.create_collection(collection_name)
            if isinstance(data, pd.DataFrame):
                collection.insert_many(data.to_dict('records'))
            elif isinstance(data, dict):
                collection.insert_one(data)
            else:
                raise ValueError("Unsupported data type for write_to_collection.")
            print(f"Data written to collection '{collection_name}' in database '{self.db.name}'")
        except Exception as e:
            print(f"Failed to write to collection '{collection_name}' in database '{self.db.name}': {e}")
            return False

    def write_to_collection(self, collection_name, data, append=True):
        try:
            uri = f"mongodb+srv://barau:1234@bdm.pqiavgt.mongodb.net/{self.db_name}?retryWrites=true&w=majority&appName=BDM"
            if not append:
                self.drop_collection(collection_name)
                self.create_collection(collection_name)
            data.write.format("mongo").option("uri", uri).option('collection', collection_name).option("encoding", "utf-8-sig").mode("append").save()
            print(f"Data written to collection '{collection_name}' in database '{self.db_name}'")
        except Exception as e:
            print(f"Failed to write to collection '{collection_name}' in database '{self.db_name}': {e}")

            return False

    @staticmethod
    def read_collection_spark(spark, host, port, db_name, collection_name):
        try:
            uri = f"mongodb://{host}:{port}/{db_name}.{collection_name}"
            df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", uri).load()
            return df
        except Exception as e:
            print(f"An error occurred while reading the collection using Spark: {collection_name}. Error: {e}")
            return None

    @staticmethod
    def write_to_collection_spark(spark, host, port, db_name, collection_name, dataframe, append=True):
        try:
            uri = f"mongodb://{host}:{port}/{db_name}.{collection_name}"
            dataframe.write.format("com.mongodb.spark.sql.DefaultSource").mode("append" if append else "overwrite").option("uri", uri).save()
            return True
        except Exception as e:
            print(f"An error occurred while writing to the collection using Spark: {collection_name}. Error: {e}")
            return False
