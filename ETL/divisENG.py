import pandas as pd

# Specify the path to your file
file_path = 'products.csv'  # replace with your path
output_prefix = 'products_part_0'  # Prefix for output file names
chunk_size = 50000  # Chunk size

# Counter for output file names
file_counter = 0

try:
    # Read the CSV file in chunks
    for chunk in pd.read_csv(file_path, sep='\t', header=0, chunksize=chunk_size, on_bad_lines='skip', low_memory=False):
        # Create the output file name
        output_file = f"{output_prefix}{file_counter}.csv"
        
        # Write the chunk to a new CSV file
        chunk.to_csv(output_file, index=False, sep='\t')  # Use tab as the delimiter
        print(f"Written {len(chunk)} rows to {output_file}.")
        
        # Increment the counter for the next file
        file_counter += 1

    print("All chunks have been processed and saved.")

except Exception as e:
    print(f"An error occurred: {e}")
