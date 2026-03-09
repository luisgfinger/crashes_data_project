import boto3
from botocore.exceptions import ClientError

def create_glue_database(database_name, description=''):

    glue_client = boto3.client('glue', region_name='sa-east-1')

    try:
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': description
            }
        )
        print(f"Database '{database_name}' created successfully.")
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            print(f"Database '{database_name}' already exists.")
        else:
            print(f"An error occurred: {e.response['Error']['Message']}")
            raise

db_name = 'crashes_nyc_db'

create_glue_database(db_name, description='A database for my project data.')
