import os
import fnmatch
import configparser
import re
import boto3
from io import BytesIO
import csv
import pymysql
import datetime
from datetime import datetime
from itertools import zip_longest

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
        


def extract_error_file_names(file_path):
    try:
        with open(file_path, 'r') as file:
            log_lines = file.read()

            # Define a regular expression pattern to match the file names
            file_name_pattern = r'File\s(.*\.json)'

            # Use re.findall to extract file names from the log lines
            file_names = re.findall(file_name_pattern, log_lines)

            return file_names
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")



def load_data_into_mysql(file_names_list, connection, schema_name, table_name):
    try:
        cursor = connection.cursor()
        print(f"Cursor established is : {cursor}")

        # Insert data into the table
        insert_query = f"INSERT INTO {schema_name}.{table_name} (filename) VALUES (%s)"
        data_to_insert = [(file_name,) for file_name in file_names_list]
        cursor.executemany(insert_query, data_to_insert)

        connection.commit()
        print(F"Data successfully loaded into MySQL {schema_name}.{table_name} table.")

    except Exception as e:
        print(f"An error occurred while loading data into MySQL {schema_name}.{table_name} : {e}")
    finally:
        if connection:
            connection.close()



if __name__ == "__main__":
    config = read_config()
    error_directory_path = config.get('Person', 'error_file')      
    error_file_name = 'error_records.txt'
    error_file_path = os.path.join(error_directory_path, error_file_name)
    print(f"error_file_path : {error_file_path}")

    error_file_names_list = extract_error_file_names(error_file_path)
    for eachFile  in error_file_names_list:
        print(f"File {eachFile} was not posted")
        
    mysql_config = {
        'host': config.get('MYSQL', 'host'),
        'user': config.get('MYSQL', 'user'),
        'password': config.get('MYSQL', 'password'),
        'database': config.get('MYSQL', 'database'),
        'pem_file_path': config.get('MYSQL', 'pem_file_path'),
        'mysql_table_name': config.get('MYSQL', 'mysql_table_name')
    }
  
    # Print configuration parameters
    connection = create_mysql_connection(mysql_config['host'], mysql_config['user'], mysql_config['password'], mysql_config['database'], mysql_config['pem_file_path'])
    print(f"Connection : {connection}")
   
    # Load data into MySQL table
    #load_data_into_mysql(error_file_names_list, your_connection, schema_name='SUD', table_name='EMPI_ERROR_RECORDS')
    
    
    
