from neo4j import GraphDatabase
import os
import time
import csv

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"  

# Define the Neo4j queries for each dataset
neo4j_queries = [
    ("Q1", "MATCH (p:d_{dataset}) WHERE p.Grade >= 70 AND p.Attendance >= 50 RETURN p"),
    ("Q2", "MATCH (p:d_{dataset}) WHERE p.Grade >= 70 RETURN p"),
    ("Q3", "MATCH (p:d_{dataset}) WHERE p.FirstName STARTS WITH 'A' RETURN p"),
    ("Q4", "MATCH (p:d_{dataset}) RETURN p")
]

datasets = ['250k', '500k', '750k', '1_million']

base_directory = r'C:\Users\HP\Downloads\Database Project\Queries_result\Neo4j'
os.makedirs(base_directory, exist_ok=True)

# Function to execute Neo4j queries
def execute_query(driver, query, dataset):
    with driver.session() as session:
        query = query.replace("{dataset}", dataset)
        execution_times = []
        for _ in range(30):
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            execution_times.append(end_time - start_time)
        return execution_times

# Establish connection to Neo4j
with GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD)) as driver:
    for dataset in datasets:
        filename = os.path.join(base_directory, f"results_{dataset}.csv")
        with open(filename, 'w', newline='') as result_file:
            csv_writer = csv.writer(result_file)
            # Write header
            csv_writer.writerow(['Query', 'Execution Times'])
            for label, query in neo4j_queries:
                execution_times = execute_query(driver, query, dataset)
                # Write query label and execution times
                csv_writer.writerow([label] + execution_times)

print("Neo4j queries execution complete.")
