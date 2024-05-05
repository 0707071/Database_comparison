import pandas as pd
from pymongo import MongoClient

def insert_data_to_mongo(file_path, collection_name):
    # Create a client object to connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["MyDatabase"]  # Access the database

    print(f"Reading CSV file from: {file_path}")
    df = pd.read_csv(file_path)
    records = df.to_dict(orient='records')
    db[collection_name].insert_many(records)
    print(f"Data inserted into {collection_name} collection.")

    client.close()  # Close the MongoDB connection

csv_file_path = 'C:/Users/HP/Downloads/Database Project/csv_mongo/'  # Note the corrected path format

insert_data_to_mongo(f'{csv_file_path}250k.csv', 'students_250k')
insert_data_to_mongo(f'{csv_file_path}500k.csv', 'students_500k')
insert_data_to_mongo(f'{csv_file_path}750k.csv', 'students_750k')
insert_data_to_mongo(f'{csv_file_path}1_million.csv', 'students_1_million')
