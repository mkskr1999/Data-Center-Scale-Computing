import pandas as pd
import sys
import numpy as np
from sqlalchemy import create_engine, text

def input_data(input_file):
    return pd.read_csv(input_file)

def transform_data(data):

    data['Month'], data['Year'] = data['MonthYear'].str.split(' ', expand=True)


    data[['reprod', 'gender']] = data['Sex upon Outcome'].str.split(' ', expand=True)
    
    data['animal_id'] = data['Animal ID']
    data['animal_name'] = data['Name']
    data['timestmp'] = data['DateTime']
    data['dob'] = data['Date of Birth']
    data['outcome_type'] = data['Outcome Type']
    data['outcome_subtype'] = data['Outcome Subtype']
    data['animal_type'] = data['Animal Type']
    data['breed'] = data['Breed']
    data['color'] = data['Color']
    data['mnth'] = data['Month']
    data['yr'] = data['Year']

    data.drop(['MonthYear', 'Age upon Outcome', 'Sex upon Outcome',
               'Animal ID', 'Name', 'DateTime', 'Date of Birth',
               'Outcome Type', 'Outcome Subtype', 'Animal Type',
               'Breed', 'Color', 'Month', 'Year'], axis=1, inplace=True)

    return data

def load_data(trans_data):
    db_url = "postgresql+psycopg2://sai:sai123@db:5432/shelter"
    conn = create_engine(db_url)
    
    
    trans_data.to_sql("temp_table", conn, if_exists="append", index=False)

    
    time_df = trans_data[['mnth', 'yr']].drop_duplicates()
    time_df[['mnth', 'yr']].to_sql("timing_dimen", conn, if_exists="append", index=False)


    animal_dim_data = trans_data[['animal_id', 'animal_type', 'animal_name', 'dob', 'breed', 'color', 'reprod', 'gender', 'timestmp']]
    animal_dim_data.to_sql("animal_dimen", conn, if_exists="append", index=False)

    
    outcome_dim_data = trans_data[['outcome_type', 'outcome_subtype']].drop_duplicates()
    outcome_dim_data.to_sql("outcome_dimen", conn, if_exists="append", index=False)

    
    join_sql = text("""
        INSERT INTO outcome_fact (outcome_dim_key, animal_dim_key, time_dim_key)
        SELECT od.outcome_dim_key, a.animal_dim_key, td.time_dim_key
        FROM temp_table o
        JOIN outcome_dimen od ON o.outcome_type = od.outcome_type AND o.outcome_subtype = od.outcome_subtype
        JOIN timing_dimen td ON o.mnth = td.mnth AND o.yr = td.yr
        JOIN animal_dimen a ON a.animal_id = o.animal_id AND a.animal_type = o.animal_type AND a.timestmp = o.timestmp;
    """)

    with conn.begin() as connection:
        connection.execute(join_sql)


if __name__ == "__main__":
    input_file = sys.argv[1]
    
    print("Start")
    data = input_data(input_file)
    transformed_data = transform_data(data)
    load_data(transformed_data)
    print("Complete")