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
    - dir_path (str): Path to the directory containing the database files.
    - db_name (str): Name of the base database file to determine the prefix.

    Returns:
    - DataFrame: Combined DataFrame with data from all database files.
    """
    try:
        all_data = []

        # Extract the base name and prefix for the database files
        base_name, ext = os.path.splitext(db_name)
        base_db_prefix = f"{base_name} "

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
        all_data = pd.concat(all_data)
        all_data = all_data.drop_duplicates()
        if not all_data.empty:
            return all_data
        else:
            return pd.DataFrame()
    except Exception as e:
        system.log_error(e)
        return pd.DataFrame()

def apply_b3_math(df, last_quarters, all_quarters):
    """Apply B3 math calculations to the DataFrame."""
    try:
        # Check the prefix of 'conta'
        conta_prefix = df['conta'].iloc[0][0]

        # Initialize index and value variables for each quarter
        i3 = i6 = i9 = i12 = None
        v3 = v6 = v9 = v12 = 0

        # Find out values for each month
        try:
            df_march = df[df['quarter'].dt.month == 3]
            if not df_march.empty:
                i3 = df_march.index[0]
                v3 = df_march['valor'].max()
        except Exception as e:
            pass
        try:
            df_june = df[df['quarter'].dt.month == 6]
            if not df_june.empty:
                i6 = df_june.index[0]
                v6 = df_june['valor'].max()
        except Exception as e:
            pass
        try:
            df_september = df[df['quarter'].dt.month == 9]
            if not df_september.empty:
                i9 = df_september.index[0]
                v9 = df_september['valor'].max()
        except Exception as e:
            pass
        try:
            df_december = df[df['quarter'].dt.month == 12]
            if not df_december.empty:
                i12 = df_december.index[0]
                v12 = df_december['valor'].max()
        except Exception as e:
            pass

        if conta_prefix in last_quarters:
            try:
                v12 = v12 - (v9 + v6 + v3)
            except Exception as e:
                print('Error in B3 math for last quarters:', e, df.iloc[0])

        if conta_prefix in all_quarters:
            try:
                v3 = v3 - 0
                v6 = v6 - (v3)
                v9 = v9 - (v6 + v3)
                v12 = v12 - (v9 + v6 + v3)
            except Exception as e:
                print('Error in B3 math for all quarters:', e, df.iloc[0])

        # Update values
        if i3 is not None:
            df.loc[i3, 'valor'] = v3
        if i6 is not None:
            df.loc[i6, 'valor'] = v6
        if i9 is not None:
            df.loc[i9, 'valor'] = v9
        if i12 is not None:
            df.loc[i12, 'valor'] = v12

        return df
    except Exception as e:
        system.log_error(e)
        return df

def save_db(df_transformed, db_file):
    """
    Save the transformed DataFrame back to the database with a 'math' suffix.

    Parameters:
    - df_transformed (DataFrame): The transformed DataFrame.
    - db_file (str): Path to the original database file.
    """
    try:
        math_db_file = db_file.replace('.db', ' math.db')
        conn = sqlite3.connect(math_db_file)
        df_transformed.to_sql('finsheet', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Saved transformed data to {math_db_file}")
    except Exception as e:
        system.log_error(e)

def process_and_save_data(db_file, last_quarters, all_quarters):
    """
    Process the data from a single database file and save the results back to the database.

    Parameters:
    - db_file (str): Path to the database file.
    - last_quarters (list): List of last quarter identifiers.
    - all_quarters (list): List of all quarter identifiers.
    """
    try:
        start_time = time.time()
        conn = sqlite3.connect(db_file)
        query = "SELECT * FROM finsheet"
        df = pd.read_sql_query(query, conn)
        conn.close()

        extra_info=[db_file]
        system.print_info(0, len(df), len(df), extra_info, start_time=start_time, size=len(df))

        # Ensure 'quarter' is in datetime format and handle any conversion errors
        df['quarter'] = pd.to_datetime(df['quarter'], errors='coerce')
        df = df.dropna(subset=['quarter'])

        # Sort the DataFrame by setor, subsetor, segmento, company, quarter, conta
        df.sort_values(by=['setor', 'subsetor', 'segmento', 'company_name', 'quarter', 'conta'], inplace=True)

        process_start_time = time.time()
        size = len(df)

        transformed_data = []
        previous_year = None
        for i, (_, group) in enumerate(df.groupby(['company_name', 'quadro', 'conta'], group_keys=False)):
            if i % (settings.batch_size * 50) == 0 and i != 0:
                save_db(pd.concat(transformed_data).reset_index(drop=True), db_file)
                transformed_data = []
                extra_info = [
                    group.iloc[0, group.columns.get_loc('company_name')],
                    group.iloc[0, group.columns.get_loc('quadro')],
                    group.iloc[0, group.columns.get_loc('conta')],
                    group.iloc[0, group.columns.get_loc('quarter')].strftime('%Y-%m-%d')
                ]
                system.print_info(i, size, settings.batch_size, extra_info, start_time=process_start_time, size=size)
            transformed_df = apply_b3_math(group, last_quarters, all_quarters)
            transformed_data.append(transformed_df)

        # Save any remaining data after the loop
        if transformed_data:
            save_db(pd.concat(transformed_data).reset_index(drop=True), db_file)

        print(f"Processed and saved {db_file} in {time.time() - start_time} seconds")
    except Exception as e:
        system.log_error(e)

def main():
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_folder = os.path.join(base_dir, settings.db_folder_short)

        last_quarters = ['3', '4']
        all_quarters = ['6', '7']

        # Extract the base name and prefix for the database files
        base_name, ext = os.path.splitext(settings.db_name)
        base_db_prefix = f"{base_name} "

        db_files = glob.glob(os.path.join(db_folder, f'{base_db_prefix}*.db'))
        valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file]

        total_files = len(valid_db_files)
        start_time = time.time()

        for i, db_file in enumerate(valid_db_files):
            system.print_info(i, total_files, total_files, extra_info=[db_file], start_time=start_time, size=total_files)
            process_and_save_data(db_file, last_quarters, all_quarters)

        print('done')
    except Exception as e:
        system.log_error(e)

if __name__ == "__main__":
    main()
