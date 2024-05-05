import os
import redis
import pandas as pd
import json

# Define Redis connection parameters
redis_host = '127.0.0.1'
redis_port = 6379
redis_password = None

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

# Directory containing CSV files
csv_directory = r'C:\Users\HP\Downloads\Database Project\csv'

# List of CSV files
csv_files = [
    '250k.csv',
    '500k.csv',
    '750k.csv',
    '1_million.csv',
]

# Iterate through CSV files
for idx, csv_file in enumerate(csv_files, start=1):
    key = f'data_set_{idx}'  # Redis key for storing data
    csv_path = os.path.join(csv_directory, csv_file)

    # Check if the file exists
    if os.path.exists(csv_path):
        # Read CSV file into a DataFrame
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')
        
        # Convert data to JSON format and insert into Redis
        r.set(key, json.dumps(data))
        
        print(f'Data from {csv_file} inserted into Redis with key: {key}')
    else:
        print(f'Error: File {csv_file} not found at path: {csv_path}')

# Close Redis connection
r.close()

print("Data insertion into Redis complete.")
