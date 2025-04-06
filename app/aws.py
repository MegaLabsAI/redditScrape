import boto3
# from app import config
import logging
import pandas as pd
import json
from io import BytesIO




class MyConfig:
    
    def __init__(self, region='us-east-1'):

        # Initialize a session without explicit credentials (it will use the EC2 instance's IAM role)
        self.session = boto3.Session(region_name=region)

        # SSM client using the session
        self.ssm = self.session.client('ssm')

        # Load AWS parameters from SSM
        self.aws_params = self.load_aws_parameters()

    def load_aws_parameters(self):
        # Define the names of the parameters you want to retrieve
        parameter_names = [
            's3_bucket', 's3_key', 'aw_access_key_id', 'aw_secret_access_key', 
            'region_name', 'red_client_id', 'red_client_secret', 
            'red_user_agent', 'OPENAI_API_KEY', 's3_conf_bucket', 's3_conf_key'
        ]

        # Retrieve the parameters in a batch
        batch_size = 10  # Maximum number of parameters that can be fetched in a single call
        all_parameters = {}

        # Split parameter names into batches of 10
        for i in range(0, len(parameter_names), batch_size):
            batch = parameter_names[i:i + batch_size]
            try:
                response = self.ssm.get_parameters(
                    Names=batch,
                    WithDecryption=True
                )
                # Store the batch parameters in the result dictionary
                parameters = {param['Name']: param['Value'] for param in response['Parameters']}
                all_parameters.update(parameters)
            except Exception as e:
                raise Exception(f"Failed to load AWS parameters for batch {batch}: {e}")

        # print(f"Loaded parameters: {all_parameters}")

        return all_parameters


class MyApp:
    def __init__(self):
        self.config = MyConfig() # Configuration loaded during initialization
      
        self.aws = self.config.aws_params
        self.s3_bucket = self.aws.get('s3_bucket').strip()
        self.s3_key = self.aws.get('s3_key').strip()
        self.s3_conf_bucket = self.aws.get('s3_conf_bucket').strip()
        self.s3_conf_key = self.aws.get('s3_conf_key').strip()
        self.openai = self.aws['OPENAI_API_KEY'].strip()
        # Initialize the S3 client as None
        self.s3 = None

    def connect_to_s3(self):
        """Create a connection to AWS S3 Bucket using the session from MyConfig."""
        if not self.s3:
            try:
                self.s3 = self.config.session.client('s3')  # Use session from MyConfig
                # print("Connected to S3.")
            except Exception as e:
                raise Exception(f"Failed to connect to AWS S3: {e}")
        return self.s3




    
    def retrieve_data_from_s3(self):
        """Read data from AWS S3 bucket and convert it to a DataFrame."""
        try:
            self.connect_to_s3()  # Ensure connection is established
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=self.s3_key)
            parquet_content = response['Body'].read()
            parquet_buffer = BytesIO(parquet_content)
            df = pd.read_parquet(parquet_buffer, engine='pyarrow')
            return df
        except Exception as e:
            raise Exception(f"An error occurred while retrieving data from S3: {e}")
        




    def retrive_redlist_from_s3(self):
        """Read data from AWS S3 bucket and convert it to a DataFrame."""
        reddit_links = None
        try:
            self.connect_to_s3()  # Ensure connection is established
            response = self.s3.get_object(Bucket=self.s3_conf_bucket, Key=self.s3_conf_key)
            
            # Read and process the object (e.g., JSON data)
            reddit_json = response['Body'].read().decode('utf-8')

            
            try:
               reddit_data = json.loads(reddit_json)
            except json.JSONDecodeError:
            # Handle the case where the JSON is invalid or cannot be parsed
                reddit_data = {}

            # Extract the list of links from the dictionary
            reddit_links = reddit_data.get("reddit_links", [])
            # print('links in views.py', reddit_links)

            # print(config_data)
        except boto3.exceptions.S3UploadFailedError as e:
            logging.error(f"Failed to retrieve object: {str(e)}")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        
            

        return reddit_links



    def upload_data_to_s3(self):
        """Upload the prepared DataFrame to AWS S3."""
        from app.data_combine import DataCombiner  # Import within the method to avoid circular import
            # Import within the method to avoid circular import

        data_combiner = DataCombiner()
        
        
        data_combine = data_combiner.combine_data()
        


        try:
            
            buffer = BytesIO()
            data_combine.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)
            self.connect_to_s3()  # Ensure connection is established
            self.s3.upload_fileobj(buffer, self.s3_bucket, self.s3_key)
            # print(f"Updated Parquet file uploaded to s3://{self.s3_bucket}/{self.s3_key}")
        except Exception as e:
            raise Exception(f"Failed to upload data to AWS S3: {e}")
        


    def update_reddit_links_in_s3(self, new_link):
        """Download, update the reddit_links JSON file, and upload it back to S3."""
        try:
            # Connect to S3
            self.connect_to_s3()
            
            # Download the existing JSON file from S3
            response = self.s3.get_object(Bucket=self.s3_conf_bucket, Key=self.s3_conf_key)
            data = response['Body'].read().decode('utf-8')

            # Load the JSON data
            json_data = json.loads(data)

            # Check if the new link already exists in the list to avoid duplicates
            if new_link not in json_data['reddit_links']:
                # Add the new link to the reddit_links list
                json_data['reddit_links'].append(new_link)
            else:
                logging.info(f"Link '{new_link}' already exists in the list.")
                return
            
            # Convert updated JSON data back to string format
            updated_data = json.dumps(json_data, indent=4)  # Format with indentation for readability
            buffer = BytesIO(updated_data.encode('utf-8'))

            # Upload the updated JSON file back to S3
            self.s3.put_object(
                Bucket=self.s3_conf_bucket,
                Key=self.s3_conf_key,
                Body=buffer,
                ContentType='application/json'  # Ensure the content type is JSON
            )
            
            logging.info(f"Successfully updated reddit_links and uploaded to s3://{self.s3_conf_bucket}/{self.s3_conf_key}")

        except Exception as e:
            logging.error(f"Error: Failed to update and upload reddit_links to S3: {e}")
            raise Exception(f"Failed to update and upload reddit_links to S3: {e}")
        finally:
        # Close the buffer
            buffer.close()

    
    def load_existing_ids(self):
    
        """Load existing Reddit post IDs from S3."""
        try:
            self.connect_to_s3()
            response = self.s3.get_object(Bucket=self.s3_bucket, Key="existing_ids.json")
            return set(json.loads(response["Body"].read().decode("utf-8")))
        except self.s3.exceptions.NoSuchKey:
            return set()

    

    def save_ids_to_s3(self, existing_ids):
        """Save updated Reddit post IDs to S3."""
        self.connect_to_s3()
        buffer = BytesIO(json.dumps(list(existing_ids)).encode("utf-8"))
        self.s3.put_object(Bucket=self.s3_bucket, Key="existing_ids.json", Body=buffer)
        buffer.close()

    
        







