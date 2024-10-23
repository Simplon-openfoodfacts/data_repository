import os
import pandas as pd

# Define column lists for Meta and Data
meta_columns = [
    'code', 'url', 'creator', 'created_t', 'created_datetime', 'last_modified_t', 
    'last_modified_datetime', 'last_modified_by', 'last_updated_t', 'last_updated_datetime'
]

data_columns = [
    'code','created_datetime','last_updated_datetime',
    'product_name',  'categories', 
    'categories_tags', 'countries_en', 'ingredients_text', 
    'ingredients_tags', 'nutriscore_score', 'nutriscore_grade', 'nova_group', 
    'nutrient_levels_tags', 'main_category', 'main_category_en', 'energy-kj_100g', 
    'energy-kcal_100g', 'energy_100g', 'fat_100g', 'saturated-fat_100g', 
    'cholesterol_100g', 'carbohydrates_100g', 'sugars_100g', 'fiber_100g', 
    'proteins_100g', 'salt_100g', 'sodium_100g', 'vitamin-c_100g', 'potassium_100g', 
    'calcium_100g', 'iron_100g', 'magnesium_100g', 'zinc_100g', 'nutrition-score-fr_100g'
]

# Path to directory with files
input_directory = os.path.join(os.getcwd(), 'Chunks')  # Chunks folder in the current directory
meta_output_directory = 'meta_chunks01'
data_output_directory = 'data_chunks01'

# Create directories for saving meta and data if they don't exist
os.makedirs(meta_output_directory, exist_ok=True)
os.makedirs(data_output_directory, exist_ok=True)

# Get a list of all files that start with 'products_part_0' in the Chunks directory
chunk_files = [f for f in os.listdir(input_directory) if f.startswith('products_part_0') and f.endswith('.csv')]

# Process each file in sequence
total_files = len(chunk_files)  # Total number of files for progress tracking
for idx, file_name in enumerate(chunk_files, 1):  # Added index to track progress
    try:
        file_path = os.path.join(input_directory, file_name)
        
        # Read CSV file with \t delimiter and low_memory=False
        df = pd.read_csv(file_path, sep='\t', low_memory=False)
        
        # Split into Meta and Data
        df_meta = df[meta_columns]
        df_data = df[data_columns]
        
        # Save Meta and Data to respective directories
        meta_file_path = os.path.join(meta_output_directory, file_name)
        df_meta.to_csv(meta_file_path, sep=',', index=False)
        
        data_file_path = os.path.join(data_output_directory, file_name)
        df_data.to_csv(data_file_path, sep=',', index=False)
        
        # Print progress message with the current file index and total
        print(f"File {file_name} successfully processed and saved in 'meta_chunks' and 'data_chunks'. [{idx}/{total_files}]")

    except Exception as e:
        print(f"Error processing file {file_name}: {e} [{idx}/{total_files}]")

print("Processing of all files completed!")
