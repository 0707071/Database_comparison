import time
import redis
import os
import csv

# Updated Redis connection settings
redis_host = '127.0.0.1'
redis_port = 6379
redis_password = None
redis_db = 0

redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

if redis_client.ping():
    print("Connected to Redis")
else:
    print("Failed to connect to Redis")

# Updated datasets with new file paths
datasets = [
    {'name': '250k', 'file_path': 'C:/Users/HP/Downloads/Database Project/csv/250k.csv'},
    {'name': '500k', 'file_path': 'C:/Users/HP/Downloads/Database Project/csv/500k.csv'},
    {'name': '750k', 'file_path': 'C:/Users/HP/Downloads/Database Project/csv/750k.csv'},
    {'name': '1million', 'file_path': 'C:/Users/HP/Downloads/Database Project/csv/1_million.csv'}
]

# Corrected folder path
base_directory = 'C:/Users/HP/Downloads/Database Project/Queries_result/Redis'

os.makedirs(base_directory, exist_ok=True)

def sanitize_query_description(description):
    return ''.join(char if char.isalnum() else '_' for char in description)

for dataset in datasets:
    set_name = dataset['name']
    print(f"Dataset: {set_name}")

    total_time = 0
    execution_times = []

    for query_index, query_description in enumerate(["Q1", "Q2", "Q3", "Q4"]):
        sanitized_query_description = sanitize_query_description(query_description)

        for i in range(30):
            start_time = time.perf_counter()

            if query_index == 0:
                all_students = redis_client.smembers(f'student_{set_name}')
            elif query_index == 1:
                name_prefix = 'J'
                students_with_name_prefix = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'FirstName').decode('utf-8').startswith(name_prefix)
                ]
            elif query_index == 2:
                min_grade = 70
                max_grade = 90
                students_with_grade_range = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if min_grade <= int(redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'Grade').decode('utf-8')) <= max_grade
                ]
            elif query_index == 3:
                min_age = 18
                min_attendance_percentage = 80
                students_with_age_and_attendance = [
                    student for student in redis_client.smembers(f'student_{set_name}')
                    if int(redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'Age').decode('utf-8')) >= min_age and
                    int(redis_client.hget(f'student:{student.decode("utf-8")}_{set_name}', 'Attendance').decode('utf-8')) >= min_attendance_percentage
                ]

            end_time = time.perf_counter()
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            total_time += execution_time

            if i == 0:
                print(f"Query {query_index + 1}, First Execution Time: {execution_time} seconds")
            if i == 29:
                avg_execution_time = total_time / 30
                print(f"Query {query_index + 1}, Average Execution Time: {avg_execution_time} seconds")

    # Corrected filename path
    filename = os.path.join(base_directory, f"results_{set_name}.csv")
    with open(filename, 'w', newline='') as result_file:
        csv_writer = csv.writer(result_file)
        csv_writer.writerow(['Query', 'Execution Times'])
        for query_num in range(1, 5):
            query_label = f"Query {query_num}"
            response_times = execution_times[(query_num - 1) * 30: query_num * 30]
            csv_writer.writerow([query_label] + response_times)

redis_client.close()
