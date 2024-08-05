import os
import glob
import time
import sqlite3
import pandas as pd

from config import settings
from utils import system

def load_db(db_file, columns=settings.cols_b3, cols=settings.cols_order):
    """
    Load data from an SQLite database into a DataFrame.
    
    This function connects to the specified SQLite database, reads the data from the 
    'finsheet' table into a DataFrame, converts the 'quarter' column to datetime format 
    and the 'version' column to numeric format. It also removes rows with empty values 
    in specified columns.

    Parameters:
    - db_file (str): Path to the SQLite database file.
    - cols (list): List of columns to check for empty values. Defaults to ['quarter', 'conta', 'valor', 'version'].

    Returns:
    - DataFrame: A DataFrame containing the data from the 'finsheet' table.
    """
    try:
        # Establish a connection to the SQLite database file
        conn = sqlite3.connect(db_file)
        
        # Read data from the 'finsheet' table into a DataFrame
        df = pd.read_sql_query("SELECT * FROM finsheet", conn)

        # Close the database connection
        conn.close()
        
        # Convert the 'quarter' column to datetime format, handling errors by setting invalid parsing to NaT
        df['quarter'] = pd.to_datetime(df['quarter'], errors='coerce')
        
        # Convert the 'version' column to numeric format, filling invalid parsing with -1 and converting to integer
        df['version'] = pd.to_numeric(df['version'], errors='coerce').fillna(-1).astype(int)

        # Remove rows with empty values in specified columns
        for col in cols:
            try:
                # Strip any whitespace from the column values and filter out empty strings
                df = df[df[col].str.strip() != ""]
            except Exception as e:
                # If an exception occurs (e.g., column not found), it is ignored and processing continues
                pass

        # Remove rows with empty 'company_name' values after converting to string and stripping whitespace
        df = df[df['company_name'].astype(str).str.strip() != ""]

        # Return the cleaned DataFrame
        return df
    except Exception as e:
        # Log any exceptions that occur during the process
        # system.log_error(e)
        return pd.DataFrame(columns=columns)

def find_new_lines(df_existing, df_new, cols=settings.cols_order):
    """
    Identify new or updated lines in the new DataFrame compared to the existing DataFrame.
    
    This function compares two DataFrames and identifies rows in the new DataFrame that 
    have a higher version number than those in the existing DataFrame or are not present in the existing DataFrame.

    Parameters:
    - df_existing (DataFrame): The existing DataFrame to compare against.
    - df_new (DataFrame): The new DataFrame with potential updates.
    - cols (list): List of columns to use for comparison. Defaults to settings.cols_order.

    Returns:
    - DataFrame: A DataFrame containing rows that are newer in df_new compared to df_existing or are not present in df_existing.
    """
    # Perform an outer merge on the two DataFrames 'df_existing' and 'df_new' based on the specified columns
    # This will create a DataFrame 'df_new_lines' with suffixes '_new' and '_old' to distinguish between columns from each DataFrame
    df_new_lines = pd.merge(df_new, df_existing, on=cols, how='outer', suffixes=('_new', '_old'), indicator=True)

    # Filter the merged DataFrame to include only rows where:
    # 1. The 'version_new' column (from 'df_new') is greater than the 'version_old' column (from 'df_existing')
    # 2. The '_merge' column indicates that the row is present in both DataFrames ('both')
    # 3. The '_merge' column indicates that the row is present only in 'df_new' ('left_only')
    df_new_lines = df_new_lines[
        ((df_new_lines['version_new'] > df_new_lines['version_old']) & (df_new_lines['_merge'] == 'both')) |
        (df_new_lines['_merge'] == 'left_only')
    ]

    # Return the filtered DataFrame containing new or updated lines
    return df_new_lines

def get_relevant_lines(df_existing, df_new):
    """
    Retrieve relevant lines from the new DataFrame based on unique combinations from new lines.
    
    This function extracts unique combinations of 'company_name', 'conta', 'tipo', and 'year' 
    from the new lines and merges them with the new DataFrame to get the relevant lines.

    Parameters:
    - df_existing (DataFrame): The existing DataFrame.
    - df_new (DataFrame): The DataFrame containing new or updated lines.

    Returns:
    - DataFrame: The relevant lines from df_existing based on new lines.
    - DataFrame: Unique combinations of 'company_name', 'conta', 'tipo', and 'year' from new lines.
    """
    try:
        # Ensure 'quarter' column is in datetime format for both DataFrames
        df_existing['quarter'] = pd.to_datetime(df_existing['quarter'])
        df_new['quarter'] = pd.to_datetime(df_new['quarter'])

        # Add a 'year' column derived from the 'quarter' column for both DataFrames
        df_existing['year'] = df_existing['quarter'].dt.year
        df_new['year'] = df_new['quarter'].dt.year

        # Get unique combinations of 'company_name', 'conta', 'tipo', and 'year' from df_new
        unique_new_lines = df_new[['company_name', 'conta', 'tipo', 'year']].drop_duplicates()

        # Merge the original DataFrame 'df_existing' with the unique new lines to get the relevant lines
        # This ensures that we are only getting the rows in 'df_existing' that match the unique new lines
        df_relevant_lines = pd.merge(df_existing, unique_new_lines, on=['company_name', 'conta', 'tipo', 'year'])
        df_relevant_lines_new = pd.merge(df_new, unique_new_lines, on=['company_name', 'conta', 'tipo', 'year'])
        
        # Drop the '_old' columns
        df_relevant_lines_new = df_relevant_lines_new[[col for col in df_relevant_lines_new.columns if '_old' not in col]]

        # Rename '_new' columns to remove the suffix
        df_relevant_lines_new.columns = [col.replace('_new', '') for col in df_relevant_lines_new.columns]

        # Save to CSV before updating
        df_relevant_lines.to_csv('df_relevant_lines.csv', index=False)
        df_relevant_lines_new.to_csv('df_relevant_lines_new.csv', index=False)

        # Update df_relevant_lines with values and versions from df_relevant_lines_new
        df_relevant_lines = pd.merge(
            df_relevant_lines,
            df_relevant_lines_new[['company_name', 'conta', 'tipo', 'year', 'quarter', 'version', 'valor']],
            on=['company_name', 'conta', 'tipo', 'year', 'quarter'],
            suffixes=('', '_new'),
            how='left'
        )

        df_relevant_lines['version'] = df_relevant_lines['version_new'].combine_first(df_relevant_lines['version'])
        df_relevant_lines['valor'] = df_relevant_lines['valor_new'].combine_first(df_relevant_lines['valor'])

        # Drop the temporary columns used for merging
        df_relevant_lines.drop(columns=['version_new', 'valor_new'], inplace=True)

        # Save the updated DataFrame back to CSV
        df_relevant_lines.to_csv('updated_df_relevant_lines.csv', index=False)

        # Return the DataFrame containing the relevant lines and the unique new lines
        return df_relevant_lines, unique_new_lines
    except Exception as e:
        # Log any exceptions that occur during the entire process
        system.log_error(e)

def group_by_conta_prefix(df):
    """
    Group the DataFrame by the first digit of the 'conta' column and apply B3 math calculations.
    
    This function groups the DataFrame by the prefix of the 'conta' column and applies 
    specific calculations to each group.

    Parameters:
    - df (DataFrame): The DataFrame to be grouped and processed.

    Returns:
    - DataFrame: The DataFrame with updated values after applying B3 math calculations.
    """
    # Check if the DataFrame 'df' is not empty
    if not df.empty:
        # Extract the first character of 'conta' as prefix and create a new column 'conta_prefix'
        df['conta_prefix'] = df['conta'].str[0]

        # Initialize a list to hold the processed DataFrames for each group
        processed_dfs = []

        # Count the total number of valid database files
        groups = df.groupby('conta_prefix')
        total_groups = len(groups)
        
        # Record the start time for processing
        start_time=  time.time()
        
        # Group the DataFrame by 'conta_prefix' and apply B3 math to each group
        for i, (conta_prefix, group) in enumerate(groups):
            # Apply the B3 math function to the group and store the result
            processed_group = b3_math(group, conta_prefix)
            processed_dfs.append(processed_group)

            # Print progress information
            extra_info = []
            system.print_info(i, 0, total_groups, extra_info, start_time=start_time, size=total_groups)

        # Combine all the processed groups into a single DataFrame
        result_df = pd.concat(processed_dfs, ignore_index=True)
        
        # Remove the 'conta_prefix' column as it is no longer needed
        result_df.drop(columns=['conta_prefix'], inplace=True)

        # Return the final processed DataFrame
        return result_df
    else:
        # If the DataFrame is empty, return
        return pd.DataFrame()

def b3_math(df, conta_prefix):
    """
    Apply B3 math calculations to the DataFrame based on the 'conta_prefix'.
    
    This function applies specific calculations to the DataFrame based on the 
    prefix of the 'conta' column, adjusting the 'valor' column for each quarter.

    Parameters:
    - df (DataFrame): The DataFrame to apply calculations on.
    - conta_prefix (str): Prefix of 'conta' to determine the calculation logic.

    Returns:
    - DataFrame: The DataFrame with updated values after applying B3 math calculations.
    """
    try:
        if conta_prefix in settings.last_quarters or conta_prefix in settings.all_quarters:
        
            # Initialize dictionaries to store the indices and values for each quarter
            indices = {'March': None, 'June': None, 'September': None, 'December': None}
            values = {'March': 0, 'June': 0, 'September': 0, 'December': 0}

            # Mapping of month numbers to quarter names
            month_to_quarter = {3: 'March', 6: 'June', 9: 'September', 12: 'December'}

            # Determine indices and max values for each quarter
            for month, quarter_name in month_to_quarter.items():
                try:
                    # Filter the DataFrame for the current month
                    df_quarter = df[df['quarter'].dt.month == month]
                    
                    # If the filtered DataFrame is not empty
                    if not df_quarter.empty:
                        # Store the index of the first row of the current quarter
                        indices[quarter_name] = df_quarter.index[0]
                        
                        # Store the maximum value of the 'valor' column for the current quarter
                        values[quarter_name] = df_quarter['valor'].max()
                except Exception as e:
                    # Log any errors that occur during this process
                    system.log_error(e)

            # Extract indices and values for each quarter
            i3, v3 = indices['March'], values['March']
            i6, v6 = indices['June'], values['June']
            i9, v9 = indices['September'], values['September']
            i12, v12 = indices['December'], values['December']

            # Define a function to apply B3 math logic based on quarter identifiers
            def apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df):
                try:
                    # If the 'conta_prefix' is in 'settings.last_quarters'
                    if conta_prefix in settings.last_quarters:
                        # Subtract the values of the previous quarters from the value of December
                        v12 -= (v9 + v6 + v3)
                    # If the 'conta_prefix' is in 'settings.all_quarters'
                    elif conta_prefix in settings.all_quarters:
                        # Adjust the values for each quarter based on the values of the previous quarters
                        v6 -= v3
                        v9 -= (v6 + v3)
                        v12 -= (v9 + v6 + v3)
                except Exception as e:
                    # Log any errors that occur during this process
                    system.log_error(e)
                
                # Return the adjusted values for each quarter
                return v3, v6, v9, v12

            # Apply the B3 math logic to the values
            v3, v6, v9, v12 = apply_b3_math_logic(v3, v6, v9, v12, conta_prefix, settings, df)

            # Define a function to update the DataFrame with the new values
            def update_dataframe(df, indices, values):
                # Loop over each quarter
                for quarter, idx in indices.items():
                    # If the index for the current quarter is not None
                    if idx is not None:
                        # Update the 'valor' column in the DataFrame with the new value for the current quarter
                        df.loc[idx, 'valor'] = values[quarter]

            # Update the DataFrame with the new values
            indices = {'March': i3, 'June': i6, 'September': i9, 'December': i12}
            values = {'March': v3, 'June': v6, 'September': v9, 'December': v12}
            update_dataframe(df, indices, values)

            # Return the updated DataFrame
        return df
    except Exception as e:
        # Log any errors that occur during the entire process
        system.log_error(e)
        # Return the original DataFrame in case of an error
        return df

def update_df(df_math, df_relevant_lines, unique_new_lines, cols=settings.cols_order):
    """
    Update the existing DataFrame with new and relevant lines.
    
    This function updates the existing DataFrame by removing rows that match unique 
    new lines and adding the relevant lines. It then sorts and removes duplicates.

    Parameters:
    - df_math (DataFrame): The existing DataFrame to be updated.
    - df_relevant_lines (DataFrame): DataFrame containing the relevant lines to add.
    - unique_new_lines (DataFrame): Unique new lines to be removed from df_math.
    - cols (list): List of columns to sort by. Defaults to settings.cols_order.

    Returns:
    - DataFrame: The updated and sorted DataFrame.
    """
    # Ensure 'quarter' column in 'df_math' is in datetime format
    df_math['quarter'] = pd.to_datetime(df_math['quarter'])

    # Add a 'year' column to 'df_math' derived from the 'quarter' column
    df_math['year'] = df_math['quarter'].dt.year

    # Remove rows from 'df_math' that are present in 'unique_new_lines'
    # Convert the relevant columns to tuples for comparison
    df_math_updated = df_math[~df_math[['company_name', 'conta', 'tipo', 'year']].apply(tuple, axis=1).isin(unique_new_lines.apply(tuple, axis=1))]

    # Concatenate the remaining rows in 'df_math' with the relevant new lines from 'df_relevant_lines'
    # Drop the 'year' column as it is no longer needed
    df_math_updated_ready = pd.concat([df_math_updated, df_relevant_lines]).drop(columns='year')

    # Sort the combined DataFrame by the specified columns and remove any duplicates
    df_math_updated_ready_sorted = df_math_updated_ready.sort_values(by=cols).drop_duplicates()

    # Return the final updated and sorted DataFrame
    return df_math_updated_ready_sorted

def save_db(df, db_file):
    """
    Save the transformed DataFrame to a new SQLite database file with a 'math' suffix.
    
    This function saves the given DataFrame to a new SQLite database file by replacing 
    the original database file extension with ' math.db'.

    Parameters:
    - df (DataFrame): The DataFrame to be saved.
    - db_file (str): Path to the original database file.
    """
    try:
        # Create a new file name for the math database by replacing the original file extension
        math_db_file = db_file.replace('.db', ' math.db')
        
        # Establish a connection to the new SQLite database file
        conn = sqlite3.connect(math_db_file)
        
        # Save the DataFrame 'df' to the 'finsheet' table in the new database
        # If the table already exists, replace it
        df.to_sql('finsheet', conn, if_exists='replace', index=False)
        
        # Close the database connection
        conn.close()

        return df
    
    except Exception as e:
        # Log any exceptions that occur during the process
        system.log_error(e)

def main():
    """
    Main function to process database files, apply transformations, and save updated data.
    
    This function processes all relevant SQLite database files in the specified folder, 
    applies data transformations, and saves the updated data to new database files.
    """
    try:
        # Get the base directory of the script
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Construct the path to the database folder using settings
        db_folder = os.path.join(base_dir, settings.db_folder_short)

        # Split the base name and extension from the database name in settings
        base_name, ext = os.path.splitext(settings.db_name)
        base_db_prefix = f"{base_name} "

        # List all database files in the specified folder that match the base name and exclude backups and math databases
        db_files = glob.glob(os.path.join(db_folder, f'{base_db_prefix}*.db'))
        valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file and 'math' not in db_file]
        
        # Count the total number of valid database files
        total_files = len(valid_db_files)
        
        # Record the start time for processing
        start_time=  time.time()

        # Process each valid database file
        for i, db_file in enumerate(valid_db_files):
            # Load the data from the current database file
            df_raw = load_db(db_file)

            # Load the data from the corresponding math database file
            df_math = load_db(db_file.replace('.db', ' math.db'))

            # # Custom update step (highlighted as a change)
            # company_name = 'EVORA SA'
            # tipo = 'DFs Consolidadas'
            # conta = '3.01'
            # quarter = '2014-12-31'
            # version = 1
            # valor = 1906037000.0 # math -1397855000.0

            # filter = (df_math['company_name'] == company_name)
            # filter &= (df_math['tipo'] == tipo)
            # filter &= (df_math['conta'] == conta)
            # filter &= (df_math['quarter'] == quarter)

            # df_math.loc[filter, 'version'] = version
            # df_math.loc[filter, 'valor'] = valor

            # Identify new or updated lines in the data
            df_new_lines = find_new_lines(df_raw, df_math)
            
            if not df_new_lines.empty:
                # Retrieve the relevant lines based on the new lines
                df_relevant_lines, unique_new_lines = get_relevant_lines(df_raw, df_new_lines)
                
                # Group the relevant lines by 'conta' prefix and apply B3 math calculations
                df_relevant_lines = group_by_conta_prefix(df_relevant_lines)

                # Update the math DataFrame with the new relevant lines
                df_math = update_df(df_math, df_relevant_lines, unique_new_lines)

                # Save the updated math DataFrame to a new SQLite database file
                df_math = save_db(df_math, db_file)

            # Print progress information
            extra_info = [f'{len(df_new_lines)} new lines updated', f"'{os.path.basename(db_file).replace('.db', ' math.db')}'", ]
            system.print_info(i, 0, total_files, extra_info, start_time=start_time, size=total_files)
        
        # Print 'done' after processing all files
        print('done')
    except Exception as e:
        # Log any exceptions that occur during the entire process
        system.log_error(e)

if __name__ == "__main__":
    main()
