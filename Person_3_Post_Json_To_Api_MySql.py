import requests
import json
import boto3
import os
import time
import configparser
import csv 



def read_config(file_path='config_person.ini'):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)
        return config
    except Exception as e:
        print(f"An error occurred while reading the configuration file: {e}")
    
   
def get_access_token(api_url, loginname, password):
    credentials = {
        "login_name": loginname,
        "password": password
    }
    response = requests.post(api_url, json=credentials)
    write_to_log(log_record_file, f" def get_access_token Response : {response}")
    try:
        response.raise_for_status()
        content_type = response.headers.get('content-type')
        if 'application/json' in content_type:
            access_token = response.json().get('access_token')
            write_to_log(log_record_file, f"def get_access_token access_token  : {access_token}")
            return access_token
        elif 'text/plain' in content_type:
            text_response = response.text
            write_to_log(log_record_file, f"def get_access_token  text_response : {text_response}")
            return text_response  # Return the text_response here
        else:
            print(f"Unexpected content type: {content_type}. Unable to handle response.")
            return None
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error: {errh}")
        write_to_log(log_record_file, f" def get_access_token Response : {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
        write_to_log(log_record_file, f" def get_access_token Response : {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
        write_to_log(log_record_file, f" def get_access_token Response : {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Request Exception: {err}")
        write_to_log(log_record_file, f" def get_access_token Response : {err}")
    print(f"Authentication failed. Status code: {response.status_code}")
    write_to_log(log_record_file, f"def get_access_token Authentication failed. Status code: {response.status_code}")
    return None
    
 
 
def post_jsonfile_to_api(target_api_url, access_token, json_file):
    #headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    headers = {
        'token': access_token,
        'Content-Type': 'application/json',
        'Accept': '*/*'
    }
    #print(headers)
    response = requests.post(target_api_url, headers=headers, json=json_file)
    write_to_log(log_record_file, f" def post_jsonfile_to_api access_token generated is : {access_token}")
    write_to_log(log_record_file, f"def post_jsonfile_to_api Response status : {response.status_code}")
    
    print(f"Response from API : {str(response.text)}")
    if response.status_code == 200:
       # Parse the JSON string
       response_dict = json.loads(str(response.text))
       is_success = True
       print(f"Response from API is_success : {is_success}")
       write_to_log(log_record_file, f"def post_jsonfile_to_api Response from API response_dict : {response_dict}")
       if not is_success:
          write_to_log(log_record_file, f'def post_jsonfile_to_api File {eachFile} was not transferred and errored out with {response_dict}')
       return is_success
    else:
       # Parse the JSON string
       response_dict = json.loads(str(response.text))
       is_success = False
       print(f"Response from API is_success : {is_success}")
       write_to_log(log_record_file, f"def post_jsonfile_to_api Response from API response_dict : {response_dict}")
       if not is_success:
          write_to_log(log_record_file, f"def post_jsonfile_to_api File {eachFile} was not transferred and errored out with {response_dict}")
       return is_success
    
    
def write_to_log(log_file, message):
    with open(log_file, 'a') as log:
        log.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def write_to_error(error_file, message):
    with open(error_file, 'a') as error:
        error.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def write_posted_records_to_csv(records, posted_records_file):
    try:
        with open(posted_records_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            for record in records:
                csv_writer.writerow([record])
                print(f"{record} was successfully written to the CSV file")
        print(f"All posted records written to {posted_records_file}")
    except Exception as e:
        print(f"An error occurred while writing posted records : {e}")

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
        
        
        
def read_api_credentials(connection, schema_name='sud', table_name='empi_api_details'):
    try:
        cursor = connection.cursor()
        # Select query to fetch api_username and api_password
        select_query = f"SELECT api_username, api_password FROM {schema_name}.{table_name}"
        cursor.execute(select_query)

        # Fetch the result
        result = cursor.fetchone()

        if result:
            api_username, api_password = result
            print(f'api_username': {api_username}, 'api_password': {api_password}')
            return api_username, api_password
        else:
            raise ValueError("No data found in the api_details table.")
    except Exception as e:
        print(f"An error occurred while reading data from MySQL: {e}")
        raise 
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    # Read the configuration
    config = read_config()
    
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
      
   
    # Access API values from the configuration
    api_auth_url = config.get('API', 'api_auth_url')
    api_target_url = config.get('API', 'api_target_url')
    #api_loginname = config.get('API', 'api_loginname')
    #api_password = config.get('API', 'api_password')
    api_loginname, api_password = read_api_credentials(connection,'sud','empi_api_details')
    
    #Access file paths from the configuration file 
    posted_file_path = config.get('Person', 'posted_json')
    error_file_path = config.get('Person', 'error_file')
    log_file_path = config.get('Person', 'log_file')
    json_file_path = config.get('Person', 'json_file_path') 
    
    
    print("\nConfig File Details \n")
    print(f'api_auth_url : {api_auth_url}')
    print(f'api_loginname : {api_loginname}')
    print(f'api_password : {api_password}')
    print(f'api_target_url : {api_target_url}')
    print(f'json_file_path : {json_file_path}')
    print(f'posted_file_path : {error_file_path}')
    print(f'error_file_path : {error_file_path}')
    print(f'log_file_path : {log_file_path}')
    print("\n")
    
    
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    
    # Creating the log file path to so that it be read
    log_file_name = "log_records.txt"
    log_record_file = os.path.join(log_file_path, log_file_name)
    
    # Creating the error file path to so that it be read
    error_file_name = "error_records.txt"
    error_record_file = os.path.join(error_file_path, error_file_name)
    
     # Creating the posted file path to so that it be read
    posted_file_name = "posted_records.txt"
    posted_records_file = os.path.join(posted_file_path, posted_file_name)
    posted_records = list()
    
    try:
        if os.path.exists(posted_records_file):
            with open(posted_records_file, "r") as file:
                lines = file.readlines()
                for each in lines:
                    posted_records.append(each.strip())
        else:
            print(f"File {posted_records_file} doesn't exists. Makse sure the script Hosp_0_Init_Cleanup.py is executed successfully") 
            write_to_log(log_record_file, f"File {posted_records_file} doesn't exists. Make sure the script Person_0_Init_Cleanup.py is executed successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

    
    #Get the list of all json files that need to be posted
    try:
        json_files = [file for file in os.listdir(json_file_path) if file.endswith('.json')]
        if not json_files:
            raise ValueError("The JSON json_files list is empty.")
            write_to_log(log_record_file, f"The JSON json_files list is empty")
    except Exception as e:
        print(f"An error occurred while processing CSV data: {e}")
    
    
    for eachFile in json_files:
        print(f"\nFile {eachFile} is in progress to be posted")
        write_to_log(log_record_file, f"\nInside for loop : File {eachFile} is in progress to be posted")
        full_file_path = os.path.join(json_file_path, eachFile)
        if eachFile not in posted_records:
            with open(full_file_path, 'r') as file:
                json_data = json.load(file)
                
                access_token = get_access_token(api_auth_url,api_loginname,api_password)
                print(f"Access token inside for loop : {access_token}")
                
                start_time = time.time()
                # Post the current file with the current access token
                success = post_jsonfile_to_api(api_target_url, access_token, json_data)
                elapsed_time = time.time() - start_time
                
                print(f"Success : {success}")
                print(f"elapsed_time : {elapsed_time}")
                # Check if the file was transferred within 60 seconds
                if elapsed_time > 60:
                    print(f"Elapsed_time {elapsed_time} : Token expired or file not transferred within 60 seconds. Regenerating token\n")
                    time.sleep(30)
                    
                    # Regenerate token
                    access_token = get_access_token(api_auth_url,api_loginname,api_password)
                    print("Token regenerated...")
                    
                    # Attempt to repost the file
                    success = post_jsonfile_to_api(api_target_url, access_token, json_data)
                    
                    if not success:
                        print(f"File {eachFile} not transferred even after token regeneration.")
                        write_to_log(log_record_file, f"Inside for loop : Failed to transfer file: {eachFile}")
                    else:
                        posted_records.append(eachFile)
                        write_posted_records_to_csv(posted_records,posted_records_file)
                        print(f"File {eachFile} not transferred even after token regeneration.\n")
                        write_to_log(log_record_file, f"Inside for loop : Successfully transferred/ added to posted records file: {eachFile}")
                elif elapsed_time < 60 and success:
                    posted_records.append(eachFile)
                    write_posted_records_to_csv(posted_records,posted_records_file)
                    write_to_log(log_record_file, f"\nInside for loop : File {eachFile} transferred within 60 seconds.")
                    print(f"Inside for loop : File {eachFile} transferred within 60 seconds.")
                else:
                    print("Inside else function")
                    print(f"Inside for loop :  File was not transferred and errored out with {success}")
                    write_to_log(log_record_file, f"File {eachFile} was not transferred and errored out with {success}")
                    write_to_error(error_record_file, f"File {eachFile} was not transferred and errored out with {success}")
        else:
             print(f'File {eachFile}  was already posted')
             write_to_log(log_record_file, f'File {eachFile}  was already posted')             
    
    
    print("\nRecords that were successfully uploaded to the API are written in the posted_record.txt")
    #write_posted_records_to_csv(posted_records,posted_records_file)
    
    print("Program completed successfully")
    
    
  
