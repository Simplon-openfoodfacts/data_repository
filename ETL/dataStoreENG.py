import os
import pandas as pd

# Path to the directory with Data files (data_chunks)
data_chunks_directory = os.path.join(os.getcwd(), 'data_chunks')

# File to save the final data
output_file = 'dataStore.csv'

# Get a list of all CSV files in the data_chunks directory
chunk_files = [f for f in os.listdir(data_chunks_directory) if f.endswith('.csv')]

# Open the file for writing final data with headers
first_file = True

# Process each file
for file_name in chunk_files:
    file_path = os.path.join(data_chunks_directory, file_name)
    
    # Read the CSV file in chunks and select 5% of each chunk
    for chunk in pd.read_csv(file_path, sep=',', chunksize=10000):  # Use chunksize to save memory
        # Select a random 5% of the data
        df_sample = chunk.sample(frac=0.05, random_state=42)
        
        # Write the sample to the final output file
        df_sample.to_csv(output_file, sep=',', index=False, mode='a', header=first_file)
        first_file = False  # After the first file, no headers are needed

    print(f"Processed file: {file_name}")

print("All files have been processed. Final data saved in dataStore.csv.")
