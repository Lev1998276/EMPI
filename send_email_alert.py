import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import mysql.connector

def read_config(file_path='config_person.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)
        return config
    except Exception as e:
        print(f"An error occurred while reading the configuration file: {e}")

def create_mysql_connection(host, user, password, database, pem_file_path):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            ssl={'ca': pem_file_path}
        )
        print(f"Connection {connection}")
        return connection

    except Exception as e:
        print(f"An error occurred: {e}")
        
        
def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    try:
        with smtplib.SMTP('smtp.vnsny.org',587) as server:
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print("Email sent successfully")
    except Exception as e:
        print(f"Error: {e}")
        

def check_records_and_send_email(connection):
    try:
        cursor = connection.cursor()
        
        schema_name = 'SUD'
        table_name = 'EMPI_ERROR_RECORDS'
        date_column = 'ERROR_TIMESTAMP'

        # Calculate the start and end of the current day
        today = datetime.now().date()
        start_of_day = datetime(today.year, today.month, today.day, 0, 0, 0)
        end_of_day = start_of_day + timedelta(days=1)

        # Query to get the number of records based on the last run
        count_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE ERROR_TIMESTAMP > COALESCE((SELECT MAX(ERROR_TIMESTAMP) FROM {table_name}), '1970-01-01 00:00:00')
            """
        cursor.execute(count_query)

        # Fetch the count
        record_count = cursor.fetchone()[0]

        if record_count < 0:
            # Records are present, no need to send an email
            print(f"Records found for {today}: {record_count}")
        else:
            # Send an email alert
            subject = "Error Records Alert"
            body = f"Error records found in {table_name} for {today}."
            send_email(subject, body)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if connection.is_connected():
            connection.close()


if __name__ == "__main__":
    config = read_config()
    mysql_config = {
        'host': config.get('MYSQL', 'host'),
        'user': config.get('MYSQL', 'user'),
        'password': config.get('MYSQL', 'password'),
        'database': config.get('MYSQL', 'database'),
        'pem_file_path': config.get('MYSQL', 'pem_file_path'),
        'mysql_table_name': config.get('MYSQL', 'mysql_table_name')
    }
    
    connection = create_mysql_connection(mysql_config['host'], mysql_config['user'], mysql_config['password'], mysql_config['database'], mysql_config['pem_file_path'])
    print(f"Connection : {connection}")

    check_records_and_send_email(connection)
