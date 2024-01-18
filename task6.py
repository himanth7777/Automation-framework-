import schedule
import time
import csv
import mysql.connector
from datetime import datetime

# Define global variables for tracking test case status
total_records = 0
success_count = 0
failure_count = 0
skipped_count = 0

# Define a function to create a report log
def create_report_log(order_id, status, error_msg):
    # Open a file and log the report
    with open('report_log.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([datetime.now(), order_id, status, error_msg])

# Define a function to create an error log for records with null values in mandatory fields
def create_error_log(order_id, error_msg):
    # Open a file and log the error
    with open('error_log.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([datetime.now(), order_id, error_msg])

# Define a function to log test case results into the database
def log_test_case_result(order_id, status, error_msg):
    mydb = mysql.connector.connect(host="localhost", user="root", passwd="Himu@6232", db="taskdemo", port=3306)
    cursor = mydb.cursor()
    create_test_case_results = '''
    CREATE TABLE IF NOT EXISTS test_case_results (
        order_id INT NOT NULL,
        status varchar(50),
        error_message varchar(50),
        execution_time date
    );
    '''
    try:
        cursor.execute(create_test_case_results)
        mydb.commit()
    except:
        print('Error occurred while creating Table')

    # Insert test case result into a database table
    insert_result_query = "INSERT INTO test_case_results (order_id, status, error_message, execution_time) VALUES (%s, %s, %s, %s)"
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(insert_result_query, (order_id, status, error_msg, current_time))
    mydb.commit()

    mydb.close()

# Define a function to create a summary report
def create_summary_report():
    global total_records, success_count, failure_count, skipped_count
    summary_msg = f"Summary: Total Records={total_records}, Success={success_count}, Failure={failure_count}, Skipped={skipped_count}"
    print(summary_msg)
    # Log the summary to a file or a dedicated section in the report_log.csv
    with open('report_log.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([datetime.now(), 'Summary', summary_msg])

# Define a function to execute SQL queries and handle multiple test cases
def execute_sql_queries(file_name, cursor):
    global total_records, success_count, failure_count, skipped_count

    # Reset counts for each run
    total_records = 0
    success_count = 0
    failure_count = 0
    skipped_count = 0

    # Read the CSV and execute SQL queries
    with open(file_name, newline='\n') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        next(csvreader)
        for row in csvreader:
            total_records += 1
            print('\n')
            print(row)
            print('Started Inserting......')
            order_id, date_of_purchase, product_id, customer_id, quantity, price = row

            # Check for null values in mandatory fields
            if any(value is None or value == '' for value in [order_id, date_of_purchase, product_id, customer_id, quantity, price]):
                skipped_count += 1
                error_msg = "Record with null values in mandatory fields. Skipped."
                create_error_log(order_id, error_msg)
                create_report_log(order_id, 'Skipped', error_msg)
                log_test_case_result(order_id, 'Skipped', error_msg)
                continue

            Date_of_loading = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql_audit_query = """INSERT INTO events_audit1 (order_id, date_of_purchase, product_id, customer_id, quantity, price, Date_of_loading) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            try:
                cursor.execute(sql_audit_query, (order_id, date_of_purchase, product_id, customer_id, quantity, price, Date_of_loading))
                mydb.commit()
                print('Insert complete......')
                success_count += 1
                create_report_log(order_id, 'Success', '')
                log_test_case_result(order_id, 'Success', '')
            except Exception as e:
                print('Error: Unable to Insert The Data!')
                failure_count += 1
                create_report_log(order_id, 'Failed', str(e))
                log_test_case_result(order_id, 'Failed', str(e))

    # Execute specified test cases
    test_cases = [
        'SELECT * FROM events_audit1',
        'SELECT COUNT(*) from events_audit1',
        'SELECT * FROM events_audit1 WHERE order_id is null or product_id IS NULL'
    ]

    for test_case in test_cases:
        try:
            cursor.execute(test_case)
            result = cursor.fetchall()
            print(f"Test Case: {test_case}")
            print(result)
        except Exception as e:
            print(f"Error executing Test Case: {test_case}")
            print(str(e))

# Define the main job function
def job():
    global mydb, cursor
    file_name = 'dataset.csv'
    create_audit_table = '''
    CREATE TABLE IF NOT EXISTS events_audit1 (
        order_id INT NOT NULL,
        date_of_purchase DATE NOT NULL,
        product_id INT NOT NULL,
        customer_id INT NOT NULL,
        quantity INT NOT NULL,
        price INT NOT NULL,
        Date_of_loading DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    '''

    cursor.execute(create_audit_table)
    mydb.commit()

    # Execute SQL queries and handle multiple test cases
    execute_sql_queries(file_name, cursor)

    create_summary_report()  # Create a summary report at the end of each run

# Set up database connection
mydb = mysql.connector.connect(host="localhost", user="root", passwd="Himu@6232", db="taskdemo", port=3306)
cursor = mydb.cursor()

# Schedule the job to run daily
schedule.every().day.at("00:05").do(job)

# Main loop to run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
