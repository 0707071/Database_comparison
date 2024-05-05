import time
import mysql.connector
import os
import csv

mysql_host = '127.0.0.1'
mysql_user = 'root'
mysql_password = 'password'
mysql_port = '3306'
mysql_database = 'ali'

table_names = ['250k', '500k', '750k', '1_million']

mysql_queries = [
    "SELECT * FROM {table_name} WHERE Grade >= 70 AND Attendance >= 50",
    "SELECT * FROM {table_name} WHERE Grade >= 70",
    "SELECT * FROM {table_name} WHERE FirstName LIKE 'A%'",
    "SELECT * FROM {table_name}"
]

base_directory = 'C:/Users/HP/Downloads/Database Project/Queries_result/Mysql'
os.makedirs(base_directory, exist_ok=True)

connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    port=mysql_port,
    database=mysql_database
)

cursor = connection.cursor()

query_execution_times = {f"Table {table_name} execution times": [] for table_name in table_names}

for table_name in table_names:
    print(f"Table: {table_name} DATASET")

    for i, query in enumerate(mysql_queries, start=1):
        formatted_query = query.format(table_name=table_name)
        execution_times = []

        for j in range(30):
            start_time = time.time()
            cursor.execute(formatted_query)
            for _ in cursor:
                pass
            end_time = time.time()
            execution_time = end_time - start_time
            execution_times.append(execution_time)

            if j == 0:
                print(f"Table: {table_name}, Query {i}, First Execution Time: {execution_time} seconds")
            if j == 29:
                avg_execution_time = sum(execution_times) / len(execution_times)
                print(f"Table: {table_name}, Query {i}, Average Execution Time: {avg_execution_time} seconds")

        query_execution_times[f"Table {table_name} execution times"].append(execution_times)

for table_name, execution_times in query_execution_times.items():
    filename = os.path.join(base_directory, f"results_{table_name.split()[1]}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for i, query in enumerate(mysql_queries, start=1):
            csv_writer.writerow([f"Query {i}"] + (execution_times[i - 1] if i - 1 < len(execution_times) else []))

cursor.close()
connection.close()
