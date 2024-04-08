import boto3
import base64
import requests
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth
import json
import sys
import io
import logging

# Initialize Spark and Glue contexts
# Define OData service URL
odata_service_base_url = "http://psms4agd01.psm.com:50000/sap/opu/odata/sap/ZDATA_PIPELINE_COEP_SRV"

# Retrieve OData service credentials from AWS Secrets Manager
secret_name = "appflow!520923320881-sap-hana-dev-1700016012318"
region_name = "us-east-1"
  
session = boto3.session.Session()
# client = session.client(service_name='secretsmanager', region_name=region_name)
# get_secret_value_response = client.get_secret_value(SecretId=secret_name)
# secret = json.loads(get_secret_value_response['SecretString'])

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

username = "APPFLOW" # secret['username']
password = "Appflow.01" # secret['password']

session = requests.Session()
session.auth = HTTPBasicAuth(username, password)

# Function to fetch the list of entities from the OData service metadata
def fetch_odata_entities(base_url):
    # metadata_url = f"{base_url}/$metadata"
    logger.info("Inside fetch_odata_entities :::")
    metadata_url = f"{base_url}"
    #response = requests.get(metadata_url, auth=HTTPBasicAuth(username, password))
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    headers = {
            "Authorization": f"Basic {credentials}"
        }

    response = requests.get(metadata_url, headers=headers)
    logger.info("After Response ::")
    if response.status_code == 200:
        entities = []
        logger.info(str("Inside response.status code"))
        logger.info(str(response.text))
        root = ET.fromstring(response.text)
        logger.info(str(root))
        for entity_set in root.findall('.//{http://www.w3.org/2007/app}collection'):
            entity_name = entity_set.get('href')
            entities.append(entity_name)
            logger.info(f"Found entity: {entity_name}")
        return entities
    else:
        raise Exception(f"Failed to fetch metadata from OData service. Status code: {response.status_code}")
        
# Function to fetch data from an OData entity and get file details
def get_file_details(base_url, entity_name):
    logger.info("get_file_details")
    # Ensure the base URL ends with a forward slash
    if not base_url.endswith('/'):
        base_url += '/'
    url = f"{base_url}{entity_name}"
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}"
    }
    logger.info("url "+ url)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Parse the XML response
        root = ET.fromstring(response.content)
        # Process the XML data as needed (e.g., extract relevant information)
        # For demonstration, just log the root tag of the XML
        logger.info(f"Root tag of the XML response: {root.tag}")

        # Your existing code for writing to a file, etc.
        # Convert DataFrame to CSV format and get size
        csv_buffer = io.StringIO()
        logger.info("File Name: " + str(entity_name))
        logger.info("File Size: " + str(len(csv_buffer.getvalue().encode('utf-8'))))

        # df.toPandas().to_csv(csv_buffer, index=False)
        with open("output.txt", "w") as file:
            # Write the contents of the StringIO object to the file
            file.write(csv_buffer.getvalue())

        # Close the StringIO object
        csv_buffer.close()
        logger.info("File write Complete")
    else:
        logger.error(f"Failed to fetch data from OData entity. Status code: {response.status_code}")
        raise Exception(f"Failed to fetch data from OData entity. Status code: {response.status_code}")

# Fetch the list of entities from the OData service
logger.info("Call fetch_odata_entities(odata_service_base_url)")
entities = fetch_odata_entities(odata_service_base_url)

# Iterate over entities and get file details
for entity in entities:
    get_file_details(odata_service_base_url, entity)
    # print(f"Entity: {entity}, File Name: {file_name}, File Size: {file_size} bytes, File Extension: {file_extension}")
    logger.info("Process Completed")
