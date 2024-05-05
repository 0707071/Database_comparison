import csv
import mysql.connector
import os

# Connection details
mysql_host = '127.0.0.1'
mysql_user = 'root'
mysql_password = 'password'
mysql_port = '3306'
mysql_database = 'ali'

# File paths
csv_paths = [
    'C:/Users/HP/Downloads/Database Project/csv/250k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/500k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/750k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/1_million.csv',
]

# Connect to MySQL server
connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    port=mysql_port,
    database=mysql_database
)

# Create table function
def create_table(cursor, table_name, header):
    columns = ', '.join([f"{col} VARCHAR(255)" for col in header])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(create_table_query)
    connection.commit()

# Insert data from CSV function
def insert_data_from_csv(cursor, table_name, file_path, header):
    create_table(cursor, table_name, header) 
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader) 
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in header])})"
        
        for row in csv_reader:
            cursor.execute(insert_query, tuple(row))

# Iterate over CSV paths and insert data into MySQL
try:
    cursor = connection.cursor()
    for csv_path in csv_paths:
        header = None
        with open(csv_path, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            header = next(csv_reader)
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
        insert_data_from_csv(cursor, table_name, csv_path, header)
        connection.commit()

    print("Data inserted successfully!")

except mysql.connector.Error as err:
    print("Error inserting data:", err)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
