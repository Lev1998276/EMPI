import os
import snowflake.connector
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import csv
import json
import configparser
from datetime import datetime


s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

CEDL_HOME = os.environ['CEDL_HOME']
nexus_connectionProfile = CEDL_HOME + '/etc/.sf.nexus.profile'
s3_connectionProfile = CEDL_HOME + '/etc/.s3_connection_profile'


def read_config(file_path='config_person.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)
        return config
    except Exception as e:
        print(f"An error occurred while reading the configuration file: {e}")
    
def read_all_csv_from_s3_folder(bucket_name, folder_name, local_directory):
    s3 = boto3.client('s3')
    # List objects in the S3 folder
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        matching_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    except Exception as e:
        print(f"Error listing objects in S3 folder: {e}")
        return []

    # Read the CSV files and create a list of dictionaries
    csv_data = []
    for file_key in matching_files:
        try:
            # Download the CSV file from S3 to a predefined local directory
            local_file = os.path.join(local_directory, f'{file_key.replace("/", "_")}')
            s3.download_file(bucket_name, file_key, local_file)

            with open(local_file, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                # Get the header from the first row
                header = next(csv_reader)
                for row in csv_reader:
                    row_dict = {header[i]: value for i, value in enumerate(row)}
                    csv_data.append(row_dict)
            #os.remove(local_file)
        except Exception as e:
            print(f"An error occurred processing file {file_key}: {e}")

    return csv_data
    
def convert_row_to_json(row):
    column_names = row.keys()
    
    detail_data = [
                    {"type": header, "value": row[header]} for header in row.keys() if header not in ['FirstName', 'LastName', 'DateOfBirth', 'keyValue']
                ]
                
    json_data = {
        "objPersonSearch":{
        "AgencyIDNumber":"00001",
        "FirstName": row['First Name'],
        "LastName": row['Last Name'],
        "MiddleName" : None,
        "DateOfBirth": row['Date of Birth'],
        "Gender": 'Male' if row['Gender'] == 'M' else 'Female' if row['Gender'] == 'F' else 'Unidentified',
        "PeopleSystemID": None,
        "People3RDPartyID": None,
        "SocialSecurityNumber": None,
        "MedicaidNumber": None,
        "MatchOverride": False 
    },
    "objUpdatePerson": {
        "method": "startautomation",
        "methodParameters": [
            {
                "type": "IS_API_CALL",
                "value": "1"
            }
        ],
        "automationKey": "VPIN_UP",
        "version": "2.0",
      "detail": [
                {
                    "id": None,
                    "foreignKeyID": None,
                    "action": "EDIT",
                    "keyValue": row['keyValue'],
                    "data": detail_data,
                    "subData": []
                }
    
    ]
    }
    }
    return json_data
    
    
def list_json_files_in_folder(folder_path):
    return [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and f.endswith('.json')]
    


if __name__ == "__main__":
    # Read the configuration
    config = read_config()
    
    csv_file_path = config.get('Person', 'csv_file_path')
    json_file_path = config.get('Person', 'json_file_path')
    s3_csv_bucket_name = config.get('Person', 's3_csv_bucket_name')
    s3_csv_bucket_name = config.get('Person', 's3_csv_bucket_name')
    s3_csv_folder_name = config.get('Person', 's3_csv_folder_name')
    predefined_directory = csv_file_path
    csv_data = read_all_csv_from_s3_folder(bucket_name, folder_name, predefined_directory)


    # Display the CSV data
    for row in csv_data:
        print(row)

   
    # Convert each csv record to JSON
    try:
        json_list = [convert_row_to_json(record) for record in csv_data if record]
        if not json_list:
            raise ValueError("The JSON list is empty.")
    except Exception as e:
        print(f"An error occurred while processing CSV data: {e}")


    counter = 1  
    for record in json_list:
        key_value = record['objUpdatePerson']['detail'][0]['keyValue']
        date_str = datetime.now().strftime("%Y%m%d")  
        file_name = f"record_{key_value}_{date_str}_{counter}.json"
        counter += 1
        # Modify the file path to include the output directory
        file_path = os.path.join(json_file_path, file_name)
        with open(file_path, "w") as json_file:
            json.dump(record, json_file, indent=2)
        print(f"JSON file dumped successfully at {file_path}")
