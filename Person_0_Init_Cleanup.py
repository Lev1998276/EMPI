import os
import shutil
import configparser
from datetime import datetime
import time



def read_config(file_path='config_person.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)
        return config
    except Exception as e:
        print(f"An error occurred while reading the configuration file: {e}")
    

def create_empty_file(directory, filename):
    try:
        # Ensure the specified directory exists
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Join the directory and filename to get the full path
        file_path = os.path.join(directory, filename)

        # Create an empty file
        with open(file_path, 'w'):
            pass

        print(f"Empty file '{filename}' created in '{directory}'.")
    except Exception as e:
        print(f"Error creating file: {e}")


if __name__ == "__main__":
    # Read the configuration
    config = read_config()
    
    posted_file_path = config.get('Person', 'posted_json')
    error_file_path = config.get('Person', 'error_file')
    log_file_path = config.get('Person', 'log_file')
    
    print(f"\nposted_file_path : {posted_file_path}")
    print(f"error_file_path : {error_file_path}")
    print(f"log_file_path : {log_file_path}\n")


    #Creating  posted file  
    #timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    posted_file_name = f"posted_records.txt"
    print(f"Creating an empty file posted_records to store the records that were successfully posted")
    create_empty_file(posted_file_path, posted_file_name)


    #Creating  error file  
    #timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    error_file_name = f"error_records.txt"
    print(f"\nCreating an empty file error_records to store the records errored out")
    create_empty_file(error_file_path, error_file_name)
    
    
    #Creating  log file  
    #timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    log_file_name = f"log_records.txt"
    print(f"\nCreating an empty file error_records to store the records errored out")
    create_empty_file(log_file_path, log_file_name)

