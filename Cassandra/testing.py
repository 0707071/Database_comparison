from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import pandas as pd
import uuid

# Define Cassandra connection parameters
cassandra_host = '127.0.0.1'
cassandra_port = 9042

# Create a Cluster instance
cluster = Cluster([cassandra_host], port=cassandra_port)

# Connect to the Cassandra cluster
session = cluster.connect()

# Define keyspace name
keyspace_name = 'your_keyspace_name'

# Create keyspace if not exists
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")

# Use the keyspace
session.execute(f"USE {keyspace_name}")

# Define table names
table_names = ['table_250k', 'table_500k', 'table_750k', 'table_1million']

# Create tables if not exists
for table_name in table_names:
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id UUID PRIMARY KEY,
            StudentID TEXT,
            FirstName TEXT,
            LastName TEXT,
            Age INT,
            Grade INT,
            Attendance INT
        )
    """)

# Function to insert data from CSV in batches
def insert_data_from_csv_batch(csv_path, table_name, batch_size=100):
    df = pd.read_csv(csv_path)
    insert_query = session.prepare(f"""
        INSERT INTO {table_name} (id, StudentID, FirstName, LastName, Age, Grade, Attendance)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    batch = None
    for index, row in df.iterrows():
        if batch is None:
            batch = BatchStatement()
        batch.add(insert_query, (uuid.uuid4(), row['StudentID'], row['FirstName'], row['LastName'], int(row['Age']), int(row['Grade']), int(row['Attendance'])))

        if index % batch_size == 0:
            session.execute(batch)
            batch = None

    if batch:
        session.execute(batch)

# CSV file paths
csv_files = [
    'C:/Users/HP/Downloads/Database Project/csv/250k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/500k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/750k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/1_million.csv'
]

# Insert data from CSV files into respective tables
for i, csv_path in enumerate(csv_files):
    table_name = table_names[i]
    print(f"Inserting data from {csv_path} into {table_name}...")
    try:
        insert_data_from_csv_batch(csv_path, table_name)
        print(f"Data insertion into {table_name} complete.")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")

# Shutdown session and cluster
session.shutdown()
cluster.shutdown()