import os
from paths import idealista_data, income_data, lookup_tables, elections_json
import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, regexp_replace, expr, explode_outer
from pyspark.sql.functions import col, count, when, max as spark_max
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType
from pyspark.sql.utils import AnalysisException
import unicodedata
from pyspark.sql.functions import col, lower, regexp_replace


class DataFormatter():
    
    def __init__(self, dfs):
        
        self.dfs = dfs
        logging.basicConfig(level=logging.INFO)

        # Append, join, and reconciliate data
        print("Reconciliating data...")
        reconciliated_dfs = self.reconciliate_data()
        print("Data reconciliated")

        # clean data for each of the reconciled dfs
<<<<<<< HEAD
        #self.dfs = {i: self.clean_data() for i, df in reconciliated_dfs.items()}
        #print("All data is clean!")

        self.dfs = reconciliated_dfs
        output_dir = "./output"  # Define your output directory here
        self.output_head(output_dir)
=======
        print("Cleaning data...")
        self.dfs = {key: self._clean_df(df) for key, df in reconciliated_dfs.items()}
        print("All data is clean!")
        
    
    # Unused function due to large amount of time taken by spark to be able to clean all the data
    # But was the main option to do
    def _clean_df_all(self, df):
        # Step 0: Remove columns with high % null values
        null_counts = {col: df.where(df[col].isNull()).count() for col in df.columns}
        total_count = df.count()
        columns_to_drop = [col for col, null_count in null_counts.items() if null_count / total_count > 0.9]
        df = df.drop(columns_to_drop)

        #Step 1: Remove rows with null values
        df = df.dropna()

        # Step 2: Remove variables with near zero variability
        numeric_columns = [col.name for col in df.schema.fields if col.dataType in [IntegerType(), DoubleType()]]
        std_devs = df.select([(expr('stddev_samp({})'.format(col))).alias(col) for col in numeric_columns]).collect()[0].asDict()
        low_std_dev_columns = [col for col, std_dev in std_devs.items() if std_dev is not None and std_dev < 0.1]
        df = df.drop(low_std_dev_columns)

        #Step 3: Remove highly unbalanced features (for categorical columns)
        categorical_columns = [col.name for col in df.schema.fields if col.dataType == StringType()]
        unbalanced_columns = []
        for col in categorical_columns:
            freq_counts = df.groupBy(col).count().orderBy(col).collect()
            if freq_counts:
                max_freq_count = freq_counts[-1][1]
                total_count = df.count()
                if max_freq_count / total_count > 0.95:
                    unbalanced_columns.append(col)
        df = df.drop(unbalanced_columns)

        return df
        

    def _clean_df(self, df):
        df = df.dropDuplicates()
        return df
>>>>>>> b3ef3318ec30b835bf4f644813bc78d9911e44d3
        
    def remove_duplicates(self):
        for key, df in self.dfs.items():
            if isinstance(df, list):
                self.dfs[key] = [sub_df.dropDuplicates() for sub_df in df]
            else:
                self.dfs[key] = df.dropDuplicates()


    def _write_df_to_csv(self, df, output_dir, filename):
        pandas_df = df.toPandas() if hasattr(df, 'toPandas') else df
        pandas_df.to_csv(os.path.join(output_dir, filename), index=False)


    def output_head(self, output_dir):
        for key, df in self.dfs.items():
            if isinstance(df, list):
                for i, sub_df in enumerate(df):
                    self._write_df_to_csv(sub_df, output_dir, f"{key}_{i}.csv")
                    if i >= 5:  # Break after writing 10 sub-dataframes
                        break
            else:
                self._write_df_to_csv(df[0] if isinstance(df, list) else df, output_dir, f"{key}_head.csv")

    def _align_dataframes_columns(self, dfs):
        
        all_columns_name = list(set().union(*(df.columns for df in dfs)))
        
        aligned_dfs = []
        for df in dfs:
            existing_columns = df.columns
            missing_columns = [col for col in all_columns_name if col not in existing_columns]
            
            for col in missing_columns:
                df = df.withColumn(col, None)
            
            df = df.select(*all_columns_name)
            aligned_dfs.append(df)
    
        return aligned_dfs
    
    def _align_df_schema(self,dfs):  
        
        df_lookup_list = self.dfs.get('lookup', [])
        df_lookup_district = None  
    
        for df in df_lookup_list:
            if 'district' in df.columns:
                df_lookup_district = df
                break 
    
        for df in dfs:
            for col_name in df.columns:
                if col_name in df_lookup_district.columns:
                    lookup_data_type = df_lookup_district.schema[col_name].dataType
                    if df.schema[col_name].dataType != lookup_data_type:
                        if isinstance(lookup_data_type, StructType):
                            df = df.withColumn(col_name, df[col_name].cast(lookup_data_type))
                        else:
                            df = df.withColumn(col_name, lit(None).cast(lookup_data_type))
        return dfs
    
    def _flatten_nested_column(self, dfs, column_name):
        
        output_dfs = []
        for df in dfs:
            if column_name in df.columns:
                nested_columns = df.select(column_name + '.*').columns
                for subcol in nested_columns:
                    df = df.withColumn(subcol, col(f"{column_name}.{subcol}"))
                df = df.drop(column_name)
            output_dfs.append(df)
        return output_dfs
<<<<<<< HEAD
    
    
    def _get_all_columns(self, dfs):
        all_columns = set()
        for df in dfs:
            all_columns.update(df.columns)
            
        return list(all_columns) if all_columns else []
    
    def lowercase_neighborhood_column(self, df):
        if 'neighborhood' in df.columns:
            df = df.withColumn('neighborhood', lower(col('neighborhood')))
            df = df.withColumn('neighborhood', regexp_replace(col('neighborhood'), '[^\w\s]', ''))
        return df
    
    def lowercase_district_column(self, df):
        if 'neighborhood' in df.columns:
            df = df.withColumn('district', lower(col('district')))
            df = df.withColumn('district', regexp_replace(col('district'), '[^\w\s]', ' '))
            df = df.withColumn('district', regexp_replace(col('district'), r'[^a-zA-Z0-9\s]', ''))

        return df
    
    
    def lowercase_neighborhood_column(self, df):
        if 'neighborhood' in df.columns:
            df = df.withColumn('neighborhood', lower(col('neighborhood')))
            df = df.withColumn('neighborhood', regexp_replace(col('neighborhood'), '[^\w\s]', ' '))
            df = df.withColumn('neighborhood', regexp_replace(col('neighborhood'), r'[^a-zA-Z0-9\s]', ''))

        return df
    


    def _join_and_union(self, dfs, df_lookup, join_column, lookup_column, ensure_same_schema=False):
        joined_list = []
        all_columns = self._get_all_columns(dfs)
        all_columns += self._get_all_columns([df_lookup])
=======
>>>>>>> b3ef3318ec30b835bf4f644813bc78d9911e44d3

    def reconciliate_data(self):
        print('Starting reconciliate process...')
        joined_dfs = {}        
        ######################## PROCESS INCOME ########################
        df_income_list = list(self.dfs.get('income').values())

        if df_income_list:
            # Drop '_id' column from each DataFrame in the list
            df_income_list = [df.drop('_id') for df in df_income_list]
            
            # Concatenate all DataFrames in the list
            df_income_combined = df_income_list[0]
            for df in df_income_list[1:]:
                df_income_combined = df_income_combined.union(df)
            
            # Load lookup tables
            df_lookup_district = self.dfs['lookup']['income_lookup_district.json'].select('district', 'district_reconciled')
            df_lookup_neighborhood = self.dfs['lookup']['income_lookup_neighborhood.json'].select('neighborhood', 'neighborhood_reconciled')
            
            # Join with district lookup
            df_income_merged = df_income_combined.join(df_lookup_district, df_income_combined['district_name'] == df_lookup_district['district'], 'left')
            df_income_merged = df_income_merged.drop('district').drop('district_name').drop('district_id').withColumnRenamed('district_reconciled', 'district')
            
            # Join with neighborhood lookup
            df_income_merged = df_income_merged.join(df_lookup_neighborhood, df_income_merged['neigh_name '] == df_lookup_neighborhood['neighborhood'], 'left')
            df_income_merged = df_income_merged.drop('neigh_name ').drop('neighborhood').withColumnRenamed('neighborhood_reconciled', 'neighborhood')
            
            # Extract and rename columns from 'info' array
            df_income_merged = df_income_merged.withColumn('RFD', expr('info[0].RFD')) \
                                            .withColumn('pop', expr('info[0].pop')) \
                                            .withColumn('year', expr('info[0].year')) \
                                            .drop('info')
            
            # Assign the processed DataFrame to joined_dfs dictionary
            joined_dfs['income'] = df_income_merged
            
            print("Finished reconciliating income!")
        
        ######################## PROCESS ELECTIONS ########################
        # Load elections data
        df_elections_list = list(self.dfs.get('elections').values())

        if df_elections_list:
            # Concatenate all DataFrames in the list
            df_elections_combined = df_elections_list[0]
            for df in df_elections_list[1:]:
                df_elections_combined = df_elections_combined.union(df)
            
            # Load lookup tables
            df_lookup_district = self.dfs['lookup']['income_lookup_district.json'].select('district', 'district_reconciled')
            df_lookup_neighborhood = self.dfs['lookup']['income_lookup_neighborhood.json'].select('neighborhood', 'neighborhood_reconciled')
            
            # Join with district lookup
            df_elections_merged = df_elections_combined.join(df_lookup_district, df_elections_combined['Nom_Districte'] == df_lookup_district['district'], 'left')
            df_elections_merged = df_elections_merged.drop('district', 'Nom_Districte', 'Codi_Districte').withColumnRenamed('district_reconciled', 'district')
            
            # Join with neighborhood lookup
            df_elections_merged = df_elections_merged.join(df_lookup_neighborhood, df_elections_merged['Nom_Barri'] == df_lookup_neighborhood['neighborhood'], 'left')
            df_elections_merged = df_elections_merged.drop('Nom_Barri', 'neighborhood').withColumnRenamed('neighborhood_reconciled', 'neighborhood')
            
            # Assign the processed DataFrame to joined_dfs dictionary
            joined_dfs['elections'] = df_elections_merged
            
            print("Finished reconciliating elections!")
            
        ######################## PROCESS IDEALISTA ########################
        # Load idealista data
        df_idealista_list = list(self.dfs.get('idealista').values())

        if df_idealista_list:
            # Drop unnecessary columns from each DataFrame in the list
            columns_to_drop = ['country', 'municipality', 'province', 'url', 'thumbnail']
            df_idealista_list = [df.drop(*columns_to_drop) for df in df_idealista_list]
            
            # Flatten nested columns
            df_idealista_list = self._flatten_nested_column(df_idealista_list, 'parkingSpace')
            df_idealista_list = self._flatten_nested_column(df_idealista_list, 'detailedType')
            df_idealista_list = self._flatten_nested_column(df_idealista_list, 'suggestedTexts')
            
            # Create a union of schemas
            all_columns = set()
            for df in df_idealista_list:
                all_columns.update(df.columns)
            all_columns = list(all_columns)
            
            # Ensure all DataFrames have the same schema
            def add_missing_columns(df, columns):
                existing_columns = df.columns
                for column in columns:
                    if column not in existing_columns:
                        df = df.withColumn(column, lit(None))
                return df

            df_idealista_list = [add_missing_columns(df, all_columns).select(all_columns) for df in df_idealista_list]

<<<<<<< HEAD
            df_idealista_list = [self.lowercase_neighborhood_column(df) for df in df_idealista_list]
            df_idealista_list = [self.lowercase_district_column(df) for df in df_idealista_list]


            # Reconcile district
            df_lookup = self.dfs['lookup']['rent_lookup_district.json'].select('di', 'di_n')
            df_idealista_merged = self._join_and_union(df_idealista_list, df_lookup, 'district', 'di_n')
            df_idealista_merged = df_idealista_merged.drop('di_n', 'district').withColumnRenamed('di', 'district')

            # Reconcile neighborhood
            df_lookup = self.dfs['lookup']['rent_lookup_neighborhood.json'].select('ne', 'ne_n')
            df_idealista_merged = self._join_and_union([df_idealista_merged], df_lookup, 'neighborhood', 'ne_n')
            df_idealista_merged = df_idealista_merged.drop('neighborhood', 'ne_n').withColumnRenamed('ne', 'neighborhood')

            print(f"Idealista after merge columns: {df_idealista_merged.columns}")
=======
            # Concatenate all DataFrames in the list
            df_idealista_combined = df_idealista_list[0]
            for df in df_idealista_list[1:]:
                df_idealista_combined = df_idealista_combined.union(df)
            
            # Load lookup tables
            df_lookup_district = self.dfs['lookup']['rent_lookup_district.json'].select('di', 'di_re')
            df_lookup_neighborhood = self.dfs['lookup']['rent_lookup_neighborhood.json'].select('ne', 'ne_re')
            
            # Join with district lookup
            df_idealista_merged = df_idealista_combined.join(df_lookup_district, df_idealista_combined['district'] == df_lookup_district['di'], 'left')
            df_idealista_merged = df_idealista_merged.drop('di', 'district').withColumnRenamed('di_re', 'district')
            
            # Join with neighborhood lookup
            df_idealista_merged = df_idealista_merged.join(df_lookup_neighborhood, df_idealista_merged['neighborhood'] == df_lookup_neighborhood['ne'], 'left')
            df_idealista_merged = df_idealista_merged.drop('neighborhood', 'ne').withColumnRenamed('ne_re', 'neighborhood')
            
            # Assign the processed DataFrame to joined_dfs dictionary
>>>>>>> b3ef3318ec30b835bf4f644813bc78d9911e44d3
            joined_dfs['idealista'] = df_idealista_merged
            
            print("Finished reconciliating idealista!")

        return joined_dfs