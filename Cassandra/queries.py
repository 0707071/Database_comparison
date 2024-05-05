import csv
import os
import time
from cassandra.cluster import Cluster

# Define Cassandra connection parameters
cassandra_host = '127.0.0.1'
cassandra_port = 9042
keyspace_name = 'your_keyspace_name'

# Create a Cluster instance
cluster = Cluster([cassandra_host], port=cassandra_port)

# Connect to the Cassandra cluster
session = cluster.connect()

# Use the keyspace
session.execute(f"USE {keyspace_name}")

table_names = ['table_250k', 'table_500k', 'table_750k', 'table_1million']

for table_name in table_names:
    
    session.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_age ON {table_name} (Age);
    """)

    query_1 = f"""
        SELECT StudentID, FirstName, LastName
        FROM {table_name}
        LIMIT 10;
    """

    query_2 = f"""
        SELECT StudentID, FirstName, LastName, Grade
        FROM {table_name}
        WHERE Grade > 90
        LIMIT 10 ALLOW FILTERING;
    """

    query_3 = f"""
        SELECT StudentID, FirstName, LastName, Age
        FROM {table_name}
        WHERE Age < 20
        LIMIT 10 ALLOW FILTERING;
    """

    query_4 = f"""
        SELECT StudentID, FirstName, LastName, Attendance
        FROM {table_name}
        LIMIT 10;
    """

    def execute_query_and_save(query, query_name, repetitions=30):
        execution_times = []

        try:
            for _ in range(repetitions):
                start_time = time.time()
                result = session.execute(query)
                execution_times.append(time.time() - start_time)

            folder_path = r"C:\Users\HP\Downloads\Database Project\Queries_result\Cassandra"
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            csv_file_path = os.path.join(folder_path, f"results_{table_name}.csv")
            is_first_execution = not os.path.exists(csv_file_path)

            with open(csv_file_path, 'a', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                if is_first_execution:
                    csv_writer.writerow(["Query Name", "Execution Times"])
                csv_writer.writerow([query_name, *execution_times])

            print(f"{query_name} Execution Times: {execution_times}")
            print(f"CSV file path: {csv_file_path}")
        except Exception as e:
            print(f"Error executing query {query_name}: {e}")

    execute_query_and_save(query_1, "Query_1")
    execute_query_and_save(query_2, "Query_2")
    execute_query_and_save(query_3, "Query_3")
    execute_query_and_save(query_4, "Query_4")

# Shutdown session and cluster
session.shutdown()
cluster.shutdown()
