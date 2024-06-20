from pyspark.sql import SparkSession
from pymongo import MongoClient
import os
from paths import idealista_data, income_data, lookup_tables, elections_data

class DataLoader:
    
    def __init__(self,spark):
        
        self.spark = spark
        
        print("Loading idealista data...")
        dfs_i = {}
        folders = os.listdir(idealista_data)
        for f in folders:
            f_path = os.path.join(idealista_data, f)
            if os.path.isdir(f_path):
                df = next(iter(self.loadToSpark(self.spark, f_path).values()))
                if df is not None:
                    dfs_i[f] = df
        if dfs_i:
            print("Idealista data loaded!")
        
        print("Loading income data...")
        df_income = self.loadToSpark(self.spark, income_data)
        print("Income data loaded!")
            
        print("Loading lookuptables data...")
        df_lookup = self.loadToSpark(self.spark, lookup_tables)
        if df_lookup is not None:
            print('Lookup tables data loaded!')

        print("Loading elections data...")
        df_elections = self.loadToSpark(self.spark, elections_data)
        if df_elections is not None:
            print('Elections data loaded!')
        
        self.dfs = {'idealista': dfs_i, 'income': df_income,'elections': df_elections ,'lookup': df_lookup}
        
        return None   
        
    
    def loadToSpark(self, spark, path):
        files = os.listdir(path)
        dfs = {}
        for f in files:
            if f.startswith('.'):
                print("Skipping hidden file: ", f)
                continue
            f_path = os.path.join(path, f)
            if f.split('.')[-1] == 'json':
                df = spark.read.option('header', True).json(f_path)
            elif f.split('.')[-1] == 'csv':
                df = spark.read.option('header', True).option('delimiter', ';').csv(f_path)

                
            elif f.split('.')[-1] == 'parquet':
                df = spark.read.option('header', True).parquet(f_path)
            else:
                df = None
                print("Uningestible file format: ", f)
            
            if df is not None:
                #df.show(5)
                dfs[f] = df
                
        return dfs
    
    