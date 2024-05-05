from neo4j import GraphDatabase
import pandas as pd
import os

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "password"

def create_label_and_insert_data(driver, label, csv_file_path):
    df = pd.read_csv(csv_file_path)
    with driver.session() as session:
        # Create label for the nodes
        session.run(f"CREATE CONSTRAINT FOR (p:{label}) REQUIRE p.StudentID IS UNIQUE")

        # Insert data into nodes with the specified label
        for index, row in df.iterrows():
            session.run(f"""
                CREATE (:{label} {{
                    StudentID: $student_id,
                    FirstName: $first_name,
                    LastName: $last_name,
                    Age: $age,
                    Grade: $grade,
                    Attendance: $attendance
                }})
            """, 
            student_id=row['StudentID'],
            first_name=row['FirstName'],
            last_name=row['LastName'],
            age=row['Age'],
            grade=row['Grade'],
            attendance=row['Attendance'])

data_set_paths = [
    'C:/Users/HP/Downloads/Database Project/csv/250k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/500k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/750k.csv',
    'C:/Users/HP/Downloads/Database Project/csv/1_million.csv'
]

labels = ['d_250k', 'd_500k', 'd_750k', 'd_1_million']

with GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD)) as driver:
    for label, csv_file_path in zip(labels, data_set_paths):
        create_label_and_insert_data(driver, label, csv_file_path)

print("Data insertion complete.")