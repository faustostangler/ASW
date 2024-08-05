import os
import glob
import time
import sqlite3
import pandas as pd
from config import settings
from utils import system

def load_existing_math_db(math_db_path):
    """
    Load data from the existing math.db file.

    Parameters:
    - math_db_path (str): Path to the existing math.db file.

    Returns:
    - DataFrame: Data from the existing math.db file, with 'quarter' column converted to datetime.
    """
    if not os.path.exists(math_db_path):
        # Return an empty DataFrame if the file does not exist
        return pd.DataFrame()
    
    # Connect to the SQLite database and read data into a DataFrame
    conn = sqlite3.connect(math_db_path)
    df = pd.read_sql_query("SELECT * FROM finsheet", conn)
    conn.close()
    
    # Convert 'quarter' column to datetime and drop rows with invalid dates
    df['quarter'] = pd.to_datetime(df['quarter'], errors='coerce')
    df.dropna(subset=['quarter'], inplace=True)
    return df

def compare_and_find_new_data(df_existing, df_new):
    """
    Compare the existing and new data to find only the new data entries.

    Parameters:
    - df_existing (DataFrame): Data from the existing math.db.
    - df_new (DataFrame): Data from the new database file.

    Returns:
    - DataFrame: Data containing only new entries.
    """
    # Define group columns for comparison
    group_columns = ['company_name', 'tipo', 'quadro', 'conta', df_new['quarter'].dt.year]
    
    # Concatenate existing and new data, then drop duplicates to identify new data
    df_combined = pd.concat([df_existing, df_new])
    df_combined.drop_duplicates(subset=group_columns, keep=False, inplace=True)
    
    # Extract new data entries
    new_data = df_combined.loc[df_combined.index.isin(df_new.index)]
    return new_data

def process_and_save_data(db_file, existing_math_db):
    """
    Process the data from a single database file and save the results back to the database.

    Parameters:
    - db_file (str): Path to the database file.
    - existing_math_db (DataFrame): Data from the existing math.db.
    """
    # Load data from the current database file into a DataFrame
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query("SELECT * FROM finsheet", conn)
    conn.close()
    
    # Convert 'quarter' column to datetime and drop rows with invalid dates
    df['quarter'] = pd.to_datetime(df['quarter'], errors='coerce')
    df.dropna(subset=['quarter'], inplace=True)

    # Compare with existing data to find new entries
    df_new = compare_and_find_new_data(existing_math_db, df)
    if df_new.empty:
        print(f"No new data in {db_file}")
        return
    
    # Group new data by specified columns
    df_new_grouped = df_new.groupby(['company_name', 'tipo', 'quadro', 'conta', df_new['quarter'].dt.year])
    
    transformed_data = []
    for _, df_group in df_new_grouped:
        # Determine the prefix of 'conta' to apply specific calculations
        conta_prefix = df_group['conta'].iloc[0][0]
        df_math = apply_b3_math(df_group.copy(), conta_prefix)
        transformed_data.append(df_math)
    
    if transformed_data:
        # Concatenate transformed data and save to the database
        df_transformed = pd.concat(transformed_data).reset_index(drop=True)
        save_db(df_transformed, db_file)

def apply_b3_math(df, conta_prefix):
    """
    Apply B3 math calculations to the DataFrame.

    Parameters:
    - df (DataFrame): DataFrame to apply calculations on.
    - conta_prefix (str): Prefix of 'conta' to determine calculation logic.

    Returns:
    - DataFrame: DataFrame with updated values based on B3 math calculations.
    """
    try:
        # Initialize dictionaries to store indices and values for each quarter
        indices = {'March': None, 'June': None, 'September': None, 'December': None}
        values = {'March': 0, 'June': 0, 'September': 0, 'December': 0}

        # Define mapping of month to quarter names
        month_to_quarter = {3: 'March', 6: 'June', 9: 'September', 12: 'December'}

        # Iterate through each quarter to find max value and index
        for month, quarter_name in month_to_quarter.items():
            try:
                df_quarter = df[df['quarter'].dt.month == month]
                if not df_quarter.empty:
                    indices[quarter_name] = df_quarter.index[0]
                    values[quarter_name] = df_quarter['valor'].max()
            except Exception as e:
                system.log_error(e)

        # Extract indices and values
        i3, v3 = indices['March'], values['March']
        i6, v6 = indices['June'], values['June']
        i9, v9 = indices['September'], values['September']
        i12, v12 = indices['December'], values['December']

        # Apply B3 math logic based on quarter identifiers
        def apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df):
            try:
                if conta_prefix in settings.last_quarters:
                    v12 -= (v9 + v6 + v3)
                elif conta_prefix in settings.all_quarters:
                    v6 -= v3
                    v9 -= (v6 + v3)
                    v12 -= (v9 + v6 + v3)
            except Exception as e:
                system.log_error(e)
            return v3, v6, v9, v12

        # Apply the logic and update values
        v3, v6, v9, v12 = apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df)
        def update_dataframe(df, indices, values):
            for quarter, idx in indices.items():
                if idx is not None:
                    df.loc[idx, 'valor'] = values[quarter]

        # Update DataFrame with new values
        indices = {'March': i3, 'June': i6, 'September': i9, 'December': i12}
        values = {'March': v3, 'June': v6, 'September': v9, 'December': v12}
        update_dataframe(df, indices, values)

        return df
    except Exception as e:
        system.log_error(e)
        return df

def save_db(df, db_file):
    """
    Save the transformed DataFrame back to the database with a 'math' suffix.

    Parameters:
    - df (DataFrame): The transformed DataFrame.
    - db_file (str): Path to the original database file.
    """
    try:
        # Create a new database file name with a 'math' suffix
        math_db_file = db_file.replace('.db', ' math.db')
        conn = sqlite3.connect(math_db_file)
        # Save the DataFrame to the new database file
        df.to_sql('finsheet', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Saved transformed data to {math_db_file}")
    except Exception as e:
        system.log_error(e)

def main():
    """
    Main function to load existing math.db, process new database files, 
    and save only new data based on comparison with existing data.
    """
    try:
        # Define base directory and data folder paths
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_folder = os.path.join(base_dir, settings.db_folder_short)

        # Extract base name and prefix for database files
        base_name, ext = os.path.splitext(settings.db_name)
        base_db_prefix = f"{base_name} "

        # List all database files matching the prefix in the specified directory
        db_files = glob.glob(os.path.join(db_folder, f'{base_db_prefix}*.db'))
        valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file and 'math' not in db_file]
        
        # Load data from existing math.db
        existing_math_db_path = os.path.join(db_folder, f"{base_name} math.db")
        existing_math_db = load_existing_math_db(existing_math_db_path)
        
        total_files = len(valid_db_files)
        start_time = time.time()

        for i, db_file in enumerate(valid_db_files):
            # Print progress information
            system.print_info(i, total_files, total_files, extra_info=[db_file], start_time=start_time, size=total_files)
            # Process and save new data
            process_and_save_data(db_file, existing_math_db)

        print('done')
    except Exception as e:
        system.log_error(e)

if __name__ == "__main__":
    main()
