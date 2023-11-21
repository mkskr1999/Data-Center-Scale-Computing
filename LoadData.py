import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCSDataLoader:
    def __init__(self, bucket_name='datacenter1798'):
        self.bucket_name = bucket_name

    def get_credentials(self):
        return {
            "type": "service_account",
            "project_id": "warm-physics-405522",
            "private_key_id": "81fa8d3a1b7492d9a69dd23d64d34a925103ed0f",
            # ... (rest of the credentials_info)
        }

    def connect_to_gcs_and_get_data(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'
        credentials_info = self.get_credentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        return self.connect_to_gcs_and_get_data(file_name)


class PostgresDataLoader:
    def __init__(self, db_config):
        self.db_config = db_config

    def get_queries(self, table_name):
        query_dict = {
            "dim_animals": """CREATE TABLE IF NOT EXISTS dim_animals (
                                animal_id VARCHAR(7) PRIMARY KEY,
                                name VARCHAR,
                                dob DATE,
                                sex VARCHAR(1), 
                                animal_type VARCHAR NOT NULL,
                                breed VARCHAR,
                                color VARCHAR
                            );""",
            "dim_outcome_types": """CREATE TABLE IF NOT EXISTS dim_outcome_types (
                                outcome_type_id INT PRIMARY KEY,
                                outcome_type VARCHAR NOT NULL
                            );""",
            "dim_dates": """CREATE TABLE IF NOT EXISTS dim_dates (
                                date_id VARCHAR(8) PRIMARY KEY,
                                date DATE NOT NULL,
                                year INT2  NOT NULL,
                                month INT2  NOT NULL,
                                day INT2  NOT NULL
                            );""",
            "fct_outcomes": """CREATE TABLE IF NOT EXISTS fct_outcomes (
                                outcome_id SERIAL PRIMARY KEY,
                                animal_id VARCHAR(7) NOT NULL,
                                date_id VARCHAR(8) NOT NULL,
                                time TIME NOT NULL,
                                outcome_type_id INT NOT NULL,
                                is_fixed BOOL,
                                FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
                                FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
                                FOREIGN KEY (outcome_type_id) REFERENCES dim_outcome_types(outcome_type_id)
                            );"""
        }
        return query_dict.get(table_name, "")

    def connect_to_postgres(self):
        return psycopg2.connect(**self.db_config)

    def create_table(self, connection, table_query):
        with connection.cursor() as cursor:
            cursor.execute(table_query)
        connection.commit()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        with connection.cursor() as cursor:
            print(f"Dropping Table {table_name}")
            truncate_table = f"DROP TABLE {table_name};"
            cursor.execute(truncate_table)
            connection.commit()

        print(f"Loading data into PostgreSQL for table {table_name}")
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcs_data)}")


def load_data_to_postgres_main(file_name, table_name):
    gcs_loader = GCSDataLoader()
    table_data_df = gcs_loader.get_data(file_name)

    db_config = {
        'dbname': 'shelterdata_db',
        'user': 'amith',
        'password': 'pgadmin17',
        'host': '34.68.213.85',
        'port': '5432',
    }

    postgres_dataloader = PostgresDataLoader(db_config)
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
