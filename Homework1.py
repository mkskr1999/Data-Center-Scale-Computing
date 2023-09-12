import sys
import pandas as pd

# loading CSV file

def process_csv(input_file, output_file):
    # Read the input CSV
    df = pd.read_csv(input_file)
    print(df.head(5))
    # Do something with the data
    df=df.drop(columns=['Unnamed: 0','Unnamed: 0.1','Unnamed: 0.2'])
    df=df.drop_duplicates()
    df=df.dropna()
    print(df.head(5))
    # Save the results to the output CSV
    df.to_csv(output_file, index=False)


if __name__ == "__main__":

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    process_csv(input_file, output_file)