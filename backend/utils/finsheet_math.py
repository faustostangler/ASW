import os
import sqlite3
import pandas as pd
import time
import glob
import shutil

from config import settings
from utils import system

def load_data(db_folder=settings.db_folder, db_name=settings.db_name):
    """
    Load data from all SQLite database files in the specified directory.

    Parameters:
    - db_folder (str): Path to the directory containing the database files.
    - db_name (str): Name of the base database file to determine the prefix.

    Returns:
    - DataFrame: Combined DataFrame with data from all database files.
    """
    try:
        all_data = []

        # Extract the base name and prefix for the database files
        base_name, ext = os.path.splitext(db_name)
        base_db_prefix = f"{base_name} "

        # List all database files matching the prefix in the specified directory
        db_files = glob.glob(os.path.join(db_folder, f'{base_db_prefix}*.db'))
        valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file]

        start_time = time.time()
        for i, db_file in enumerate(valid_db_files):
            if ' BENS INDUSTRIAIS' in db_file:
                conn = sqlite3.connect(db_file)
                query = "SELECT * FROM finsheet"
                df = pd.read_sql_query(query, conn)
                conn.close()
                all_data.append(df)
            system.print_info(i, 0, len(valid_db_files), extra_info=[db_file], start_time=start_time, size=len(valid_db_files))
        
        # Concatenate all dataframes into one
        all_data = pd.concat(all_data)
        # Remove duplicate rows
        all_data = all_data.drop_duplicates()
        if not all_data.empty:
            return all_data
        else:
            return pd.DataFrame()
    except Exception as e:
        system.log_error(e)
        return pd.DataFrame()

def apply_b3_math(df, conta_prefix):
    """Apply B3 math calculations to the DataFrame."""
    try:
        # Initialize index and value variables for each quarter
        indices = {'March': None, 'June': None, 'September': None, 'December': None}
        values = {'March': 0, 'June': 0, 'September': 0, 'December': 0}

        # Define a mapping of month to quarter names
        month_to_quarter = {3: 'March', 6: 'June', 9: 'September', 12: 'December'}

        # Iterate through each quarter to find the max value and index
        for month, quarter_name in month_to_quarter.items():
            try:
                df_quarter = df[df['quarter'].dt.month == month]
                if not df_quarter.empty:
                    indices[quarter_name] = df_quarter.index[0]
                    values[quarter_name] = df_quarter['valor'].max()
            except Exception as e:
                pass

        # Access the results
        i3, v3 = indices['March'], values['March']
        i6, v6 = indices['June'], values['June']
        i9, v9 = indices['September'], values['September']
        i12, v12 = indices['December'], values['December']

        # Function to apply B3 math logic based on quarter identifiers
        def apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df):
            try:
                if conta_prefix in settings.last_quarters:
                    v12 -= (v9 + v6 + v3)
                elif conta_prefix in settings.all_quarters:
                    v6 -= v3
                    v9 -= (v6 + v3)
                    v12 -= (v9 + v6 + v3)
            except Exception as e:
                print(f'Error in B3 math for {"last quarters" if conta_prefix in settings.last_quarters else "all quarters"}:', e, df.iloc[0])
            return v3, v6, v9, v12

        # Apply the logic
        v3, v6, v9, v12 = apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df)

        # Update values in the DataFrame
        def update_dataframe(df, indices, values):
            for quarter, idx in indices.items():
                if idx is not None:
                    df.loc[idx, 'valor'] = values[quarter]

        # Prepare indices and values dictionaries
        indices = {'March': i3, 'June': i6, 'September': i9, 'December': i12}
        values = {'March': v3, 'June': v6, 'September': v9, 'December': v12}

        # Update the DataFrame
        update_dataframe(df, indices, values)

        return df
    except Exception as e:
        system.log_error(e)
        return df

def save_db(df, db_file):
    """
    Save the transformed DataFrame back to the database with a 'math' suffix.

    Parameters:
    - df_transformed (DataFrame): The transformed DataFrame.
    - db_file (str): Path to the original database file.
    """
    try:
        # Create a new database file name with a 'math' suffix
        math_db_file = db_file.replace('.db', ' math.db')
        conn = sqlite3.connect(math_db_file)
        # Save the DataFrame to the new database file
        df.to_sql('finsheet', conn, if_exists='replace', index=False)
        conn.close()
        # print(f"Saved transformed data to {math_db_file}")
    except Exception as e:
        system.log_error(e)

def process_and_save_data(db_file):
    """
    Process the data from a single database file and save the results back to the database.

    Parameters:
    - db_file (str): Path to the database file.
    """
    try:
        conn = sqlite3.connect(db_file)
        query = "SELECT * FROM finsheet"
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Ensure 'quarter' is in datetime format and handle any conversion errors
        df['quarter'] = pd.to_datetime(df['quarter'], errors='coerce')
        df = df.dropna(subset=['quarter'])

        # Sort the DataFrame by setor, subsetor, segmento, company, quarter, conta
        df.sort_values(by=['setor', 'subsetor', 'segmento', 'company_name', 'quarter', 'conta'], inplace=True)

        # Split df into two parts: relevant subset for processing and the rest
        group_columns = ['company_name', 'tipo', 'quadro', 'conta', df['quarter'].dt.year]
        df_relevant = df[df['conta'].str[0].isin(settings.last_quarters + settings.all_quarters) & (df.groupby(group_columns)['conta'].transform('count') == 4)]
        df_remainder = df[~df.index.isin(df_relevant.index)]

        df_grouped = df_relevant.groupby(group_columns, group_keys=False)

        size = df_grouped.ngroups
        process_start_time = time.time()
        transformed_data = []
        for i, (_, df) in enumerate(df_grouped):
            # Check the prefix of 'conta' to determine the calculation logic
            conta_prefix = df['conta'].iloc[0][0]

            df_math = apply_b3_math(df.copy(), conta_prefix)
            transformed_data.append(df_math)
            if i % (settings.batch_size * 20) == 0:
                # Save the transformed data in batches to avoid memory issues
                save_db(pd.concat(transformed_data).reset_index(drop=True), db_file)
                extra_info = [
                    df.iloc[0, df.columns.get_loc('tipo')],
                    df.iloc[0, df.columns.get_loc('company_name')],
                    df.iloc[0, df.columns.get_loc('quadro')],
                    df.iloc[0, df.columns.get_loc('quarter')].strftime('%Y-%m-%d'), 
                    df.iloc[0, df.columns.get_loc('conta')]
                ]
                system.print_info(i, size, settings.batch_size, extra_info, start_time=process_start_time, size=size)

        # Save any remaining data after the loop
        if transformed_data:
            save_db(pd.concat(transformed_data).reset_index(drop=True), db_file)

        # Combine the transformed data with the remainder
        if transformed_data:
            df_transformed = pd.concat(transformed_data + [df_remainder]).reset_index(drop=True)
        else:
            df_transformed = df_remainder
        
        df_transformed.sort_values(by=['setor', 'subsetor', 'segmento', 'company_name', 'quarter', 'conta'], inplace=True)
        save_db(df_transformed, db_file)
        print(f"Processed and saved {db_file} in {time.time() - process_start_time} seconds")
    except Exception as e:
        system.log_error(e)

def main():
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_folder = os.path.join(base_dir, settings.db_folder_short)

        # Extract the base name and prefix for the database files
        base_name, ext = os.path.splitext(settings.db_name)
        base_db_prefix = f"{base_name} "

        # List all database files matching the prefix in the specified directory
        db_files = glob.glob(os.path.join(db_folder, f'{base_db_prefix}*.db'))
        valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file and 'math' not in db_file]

        total_files = len(valid_db_files)
        start_time = time.time()

        for i, db_file in enumerate(valid_db_files):
            system.print_info(i, total_files, total_files, extra_info=[db_file], start_time=start_time, size=total_files)
            process_and_save_data(db_file)

        print('done')
    except Exception as e:
        system.log_error(e)

if __name__ == "__main__":
    main()
