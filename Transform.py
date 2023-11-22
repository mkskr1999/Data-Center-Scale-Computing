import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')

# Creating the global mapping for outcome types
outcomes_map = {
    'Rto-Adopt': 1, 
    'Adoption': 2, 
    'Euthanasia': 3, 
    'Transfer': 4,
    'Return to Owner': 5, 
    'Died': 6,
    'Disposal': 7,
    'Missing': 8,
    'Relocate': 9,
    'N/A': 10,
    'Stolen': 11
}

def get_credentials():
    bucket_name = "datacenter1798"
    credentials_info = {
        "type": "service_account",
        # ... (credentials_info remains the same)
    }
    return credentials_info, bucket_name

def connect_to_gcs_and_get_data(credentials_info, gcs_bucket_name):
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    client = storage.Client.from_service_account_info(credentials_info)
    bucket = client.get_bucket(gcs_bucket_name)
    
    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    # Format the file path with the current date
    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    # Read the CSV file from GCS into a DataFrame
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df

def write_data_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCS.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")
    
def prep_data(data):
    # ... (unchanged)
    return data

def prep_animal_dim(data):
    # ... (unchanged)
    return animal_dim.drop_duplicates()

def prep_date_dim(data):
    # ... (unchanged)
    return dates_dim.drop_duplicates()

def prep_outcome_types_dim(data):
    # ... (unchanged)
    return outcome_types_dim

def prep_outcomes_fct(data):
    # ... (unchanged)
    return outcomes_fct

def transform_data():
    credentials_info, bucket_name = get_credentials()
    new_data = connect_to_gcs_and_get_data(credentials_info, bucket_name)
    new_data = prep_data(new_data)
    
    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)
    fct_outcomes = prep_outcomes_fct(new_data)

    dim_animal_path = "transformed_data/dim_animal.csv"
    dim_dates_path = "transformed_data/dim_dates.csv"
    dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
    fct_outcomes_path = "transformed_data/fct_outcomes.csv"

    write_data_to_gcs(dim_animal, credentials_info, bucket_name, dim_animal_path)
    write_data_to_gcs(dim_dates, credentials_info, bucket_name, dim_dates_path)
    write_data_to_gcs(dim_outcome_types, credentials_info, bucket_name, dim_outcome_types_path)
    write_data_to_gcs(fct_outcomes, credentials_info, bucket_name, fct_outcomes_path)
