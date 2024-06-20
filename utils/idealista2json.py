import json
import os
import pandas as pd
from paths import idealista_data, idealista_json
from paths import elections_data, elections_json

def get_subfolders(folder):
    subfolders = [f.path for f in os.scandir(folder) if f.is_dir()]
    return subfolders

def parquet2json(input_folder, output_folder):
    # Get the subfolders for the idealista folder
    subfolders = get_subfolders(input_folder)
    
    for subfolder in subfolders:
        subfolder_path = os.path.join(input_folder, subfolder)
        parquet_files = [f for f in os.listdir(subfolder_path) if f.endswith('.parquet')]
        
        if len(parquet_files) == 1:
            parquet_file = os.path.join(subfolder_path, parquet_files[0])
            try:
                df = pd.read_parquet(parquet_file)
                json_records = df.to_dict(orient='records')
                date = subfolder[:-10]
                output_file = os.path.join(output_folder, f'{date}.json')
                print(output_file)
                with open(parquet_file, 'w', encoding='utf-8-sig') as file:
                    json.dump(json_records, file, ensure_ascii=False)
                #print(f"Successfully converted {parquet_file} to {output_file}")
            except Exception as e:
                print(f"Error processing {parquet_file}: {e}")
        else:
            print(f"No or multiple Parquet files found in {subfolder}")
            
def csv2json(input_folder, output_folder):
    # List all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        csv_file_path = os.path.join(input_folder, csv_file)
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(csv_file_path)
            # Convert the DataFrame to a list of dictionaries
            json_records = df.to_dict(orient='records')
            # Generate the output file path
            file_name, _ = os.path.splitext(csv_file)  # Extract the file name without extension
            output_file = os.path.join(output_folder, f'{file_name}.json')
            # Write the JSON data to the output file
            with open(output_file, 'w', encoding='utf-8-sig') as file:
                json.dump(json_records, file, ensure_ascii=False, indent=4)
            print(f"Successfully converted {csv_file_path} to {output_file}")
        except Exception as e:
            print(f"Error processing {csv_file_path}: {e}")


def main():
    parquet2json(idealista_data, idealista_json)
    #csv2json(elections_data,elections_json)

if __name__ == "__main__":
    main()