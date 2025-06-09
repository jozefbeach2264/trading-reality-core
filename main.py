import pandas as pd
import os
import glob

# Define the base schema for your trading model (required and optional fields)
BASE_SCHEMA = {
    'required': ['unix', 'close'],  # Minimum fields your model needs
    'optional': ['date', 'symbol', 'open', 'high', 'low', 'volume', 'volume eth', 'volume usdt', 'tradecount']
}

def load_data(file_path):
    try:
        print(f"Loading {os.path.basename(file_path)} in chunks...")
        df = pd.read_csv(file_path, chunksize=10000, encoding='utf-8-sig', on_bad_lines='skip', low_memory=False)
        chunks = []
        for chunk in df:
            # Store original column names for logging
            original_columns = chunk.columns.tolist()
            # Normalize column names to lowercase and strip spaces
            chunk.columns = [col.lower().strip() for col in chunk.columns]
            chunks.append(chunk)
        data = pd.concat(chunks, ignore_index=True)
        print(f"Total data loaded from {os.path.basename(file_path)}: {len(data)} rows, {len(data.columns)} columns")
        print(f"Original columns: {original_columns}")
        print(f"Normalized columns: {data.columns.tolist()}")  # Debug
        return data, original_columns
    except Exception as e:
        print(f"Error loading {os.path.basename(file_path)}: {str(e)}")
        return None, None

def parse_schema(data, original_columns, file_name):
    """Parse and validate the schema against the base schema."""
    normalized_columns = data.columns.tolist()
    schema = {'file': file_name, 'columns': normalized_columns}
    
    # Check required fields
    missing_required = [col for col in BASE_SCHEMA['required'] if col not in normalized_columns]
    if missing_required:
        print(f"Schema error in {file_name}: Missing required columns {missing_required}")
        return None, schema
    
    # Map original to normalized columns for reference
    schema['mapping'] = {orig: norm for orig, norm in zip(original_columns, normalized_columns)}
    print(f"Schema parsed for {file_name}: {schema}")
    return schema, schema

def preprocess_data(data, schema, file_name):
    """Preprocess data with schema-bound outputs."""
    try:
        print(f"Starting Dan1 and Dan2 for {file_name} with schema: {schema['columns']}")
        
        # Enforce schema conformity
        for col in BASE_SCHEMA['required']:
            if col not in data.columns:
                raise ValueError(f"Schema violation in {file_name}: Required column '{col}' not found after mapping")
        
        # Example Dan1 logic: Convert numeric fields and drop NaNs
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'volume eth', 'volume usdt', 'tradecount']
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        data = data.dropna(subset=BASE_SCHEMA['required'])  # Drop rows missing required data
        
        print(f"Dan1: Preprocessing successful for {file_name}")
        # Add your Dan2 logic here if needed
        return data
    except Exception as e:
        print(f"Error in preprocessing {file_name}: {str(e)}")
        return None

def main():
    # Get the root directory
    root_dir = os.getcwd()
    print(f"Scanning for CSV files in: {root_dir}")

    # Find all CSV files in the root directory
    csv_files = glob.glob(os.path.join(root_dir, "*.csv"))
    if not csv_files:
        print("No CSV files found in the root directory.")
        return

    print(f"Found {len(csv_files)} CSV files: {[os.path.basename(f) for f in csv_files]}")

    # Process each CSV file
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        try:
            # Load the data and original columns
            data, original_columns = load_data(file_path)
            if data is None or data.empty:
                print(f"Skipping {file_name} due to loading error or empty data")
                continue

            # Parse and validate schema
            schema, validated_schema = parse_schema(data, original_columns, file_name)
            if schema is None:
                print(f"Skipping {file_name} due to schema error")
                continue

            # Preprocess the data with schema binding
            processed_data = preprocess_data(data, validated_schema, file_name)
            if processed_data is None or processed_data.empty:
                print(f"Skipping {file_name} due to preprocessing error or empty data")
                continue

            # Save the processed data with schema-compliant structure
            output_file = f"processed_{file_name}"
            processed_data.to_csv(output_file, index=False)
            print(f"Processed data saved to {output_file} with schema: {validated_schema['columns']}")

        except Exception as e:
            print(f"Main error for {file_name}: {str(e)}")
            continue

if __name__ == "__main__":
    main()