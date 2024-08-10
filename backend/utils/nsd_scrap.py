import os
import glob
import time
import sqlite3
import pandas as pd
from config import settings
from utils import system

def load_db(db_path, columns=settings.cols_b3):
    """
    Load data from a specified database file.

    Parameters:
    - db_path (str): Path to the database file.

    Returns:
    - DataFrame: Data from the specified database file with the 'quarter' column converted to datetime.
    """
    # Hard-coded variables
    quarter_column = 'quarter'

    # Check if the database file exists
    if not os.path.exists(db_path):
        # If the file doesn't exist, return an empty DataFrame
        return pd.DataFrame(columns=columns)
    
    # Connect to the SQLite database and load the data into a DataFrame
    conn = sqlite3.connect(db_path)
    df_existing = pd.read_sql_query("SELECT * FROM finsheet", conn)
    conn.close()
    
    # Convert the 'quarter' column to datetime format and remove rows with invalid dates
    df_existing[quarter_column] = pd.to_datetime(df_existing[quarter_column], errors='coerce')
    df_existing.dropna(subset=[quarter_column], inplace=True)
    
    # Return the processed DataFrame
    return df_existing

def identify_records(db_file_path):
    """
    Identify both new and already calculated records by comparing existing math.db (calculated) data with current db (new) data.

    Parameters:
    - db_file_path (str): Path to the database file.

    Returns:
    - Tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
      - df_to_calculate: DataFrame containing only the new records that need calculation.
      - df_calculated: DataFrame containing only the already calculated records.
    """
    try:
        # Hard-coded variables
        key_columns = ['company_name', 'tipo', 'quadro', 'conta', 'year']
        sort_columns = ['company_name', 'quarter', 'tipo', 'conta']
        
        # Load the current data from the regular database file
        new_data = load_db(db_file_path)
        new_data = new_data[settings.cols_b3].copy()  # Use only relevant columns

        # Load the existing calculated data from the corresponding math.db file
        calculated_data = load_db(db_file_path.replace('.db', ' math.db'))
        calculated_data = calculated_data[settings.cols_b3].copy()  # Use only relevant columns
        
        # Add 'year' column and 'conta_prefix' column if the DataFrame is not empty
        if not calculated_data.empty:
            calculated_data['year'] = calculated_data['quarter'].dt.year
            calculated_data['conta_prefix'] = calculated_data['conta'].str[0]
        else:
            calculated_data['year'] = pd.Series(dtype='int')
            calculated_data['conta_prefix'] = pd.Series(dtype='str')

        if not new_data.empty:
            new_data['year'] = new_data['quarter'].dt.year
            new_data['conta_prefix'] = new_data['conta'].str[0]
        else:
            new_data['year'] = pd.Series(dtype='int')
            new_data['conta_prefix'] = pd.Series(dtype='str')

        # Identify records that do NOT require special calculations as no_math_new_data (not in last_quarters nor in all_quarters)
        no_math_new_data = new_data[new_data['conta_prefix'].isin(
            set(new_data['conta_prefix']) - set(settings.last_quarters) - set(settings.all_quarters)
        )]

        # Identify records that DO require special calculations as math_needed
        math_needed = new_data[~new_data.index.isin(no_math_new_data.index)]

        # Merge the current and existing data to find both new and calculated records
        merged_data = pd.merge(math_needed, calculated_data, on=key_columns, how='outer', indicator=True, suffixes=('', '_existing'))

        # Identify records that need calculation: present in new_data but not in calculated_data
        df_to_calculate = merged_data[merged_data['_merge'] == 'left_only'].drop(columns=['_merge'])
        df_to_calculate = df_to_calculate[settings.cols_b3].sort_values(by=sort_columns)

        # Ensure 'year' column is added to df_to_calculate before merging
        if not df_to_calculate.empty:
            df_to_calculate['year'] = df_to_calculate['quarter'].dt.year

        # Perform an anti-join to remove any records from df_calculated that are present in df_to_calculate
        df_calculated = pd.merge(calculated_data, df_to_calculate, on=key_columns, how='left', indicator=True, suffixes=('', '_existing'))
        df_calculated = df_calculated[df_calculated['_merge'] == 'left_only'].drop(columns=['_merge'])
        df_calculated = df_calculated[settings.cols_b3].sort_values(by=sort_columns)

        # Add no_math_new_data records to df_calculated
        df_calculated = pd.concat([df_calculated, no_math_new_data])
        df_calculated = df_calculated[settings.cols_b3].sort_values(by=sort_columns)

        return df_to_calculate[settings.cols_b3], df_calculated[settings.cols_b3]
    
    except Exception as e:
        # system.log_error(e)
        return pd.DataFrame(columns=settings.cols_b3), pd.DataFrame(columns=settings.cols_b3)

def process_database(db_file_path):
    """
    Process data from a single database file and return the processed and transformed results.

    Parameters:
    - db_file_path (str): Path to the database file.

    Returns:
    - DataFrame: DataFrame containing the final processed and transformed data.
    """
    try:
        # Column name for quarter and columns used for grouping data
        grouping_columns = ['company_name', 'tipo', 'quadro', 'conta', 'quarter']
        sort_columns = ['company_name', 'quarter', 'tipo', 'conta']

        # Identify new records that need processing and already calculated records
        records_to_calculate, already_calculated_records = identify_records(db_file_path)
        
        # If there are no new records, return the already calculated records
        if records_to_calculate.empty:
            return already_calculated_records

        # Group the new records by the specified columns to prepare for mathematical processing
        records_to_calculate = records_to_calculate[settings.cols_b3].sort_values(by=sort_columns)
        grouped_records = records_to_calculate.groupby(grouping_columns)
        
        # List to hold all the processed data
        calculated_data_groups = []
        accumulated_data = []  # List to accumulate all processed data before clearing
        total_groups = len(grouped_records)
        start_time = time.time()

        # Process each group, applying the necessary mathematical transformations
        for index, (_, group) in enumerate(grouped_records):
            # Extract the first digit of the 'conta' to determine the calculation logic
            conta_prefix = group['conta'].iloc[0][0]
            row = group.iloc[0]

            # Apply the mathematical transformations specific to B3 to the group
            calculated_group = apply_b3_math(group.copy(), conta_prefix)
            calculated_data_groups.append(calculated_group)

            # Print progress information and save data at regular intervals or at the end
            if (total_groups - index - 1) % (settings.batch_size * 20 * 5) == 0 or index == total_groups - 1:
                extra_info = [row['company_name'], row['quarter'].strftime('%Y-%m-%d')]
                system.print_info(index, total_groups, total_groups, extra_info, start_time=start_time, size=total_groups)

                # Concatenate processed data groups and save to the database
                partial_transformed_data = pd.concat(calculated_data_groups).reset_index(drop=True)
                partial_transformed_data = save_db(partial_transformed_data, db_file_path.replace('.db', ' math.db'))
                
                # Accumulate the saved data before clearing
                accumulated_data.append(partial_transformed_data)
                
                # Clear the list to free memory after saving
                calculated_data_groups.clear()

        # Filter out empty or all-NA DataFrames before concatenating
        non_empty_accumulated_data = [df for df in accumulated_data if not df.empty and not df.isna().all().all()]

        # Combine all accumulated data with the already calculated records
        if non_empty_accumulated_data:
            final_result = pd.concat(non_empty_accumulated_data + [already_calculated_records]).reset_index(drop=True)
            return final_result

    except Exception as e:
        # Log any errors that occur during processing
        system.log_error(e)
        return pd.DataFrame()

def apply_b3_math(df_group, conta_prefix):
    """
    Apply B3-specific mathematical transformations to the data.

    Parameters:
    - df_group (DataFrame): DataFrame to apply calculations on.
    - conta_prefix (str): Prefix of 'conta' to determine calculation logic.

    Returns:
    - DataFrame: DataFrame with updated values based on B3 math calculations.
    """
    try:
        # Hard-coded variables
        quarter_columns = ['March', 'June', 'September', 'December']
        month_to_quarter_map = {3: 'March', 6: 'June', 9: 'September', 12: 'December'}
        quarter_column = 'quarter'
        value_column = 'valor'

        # Check if the 'conta' prefix indicates that B3 math should be applied
        if conta_prefix in settings.last_quarters or conta_prefix in settings.all_quarters:
            # Initialize dictionaries to store the indices and values for each quarter
            quarter_indices = {quarter: None for quarter in quarter_columns}
            quarter_values = {quarter: 0 for quarter in quarter_columns}

            # Iterate over each quarter and find the maximum value for each
            for month, quarter_name in month_to_quarter_map.items():
                try:
                    df_quarter = df_group[df_group[quarter_column].dt.month == month]
                    if not df_quarter.empty:
                        quarter_indices[quarter_name] = df_quarter.index[0]
                        quarter_values[quarter_name] = df_quarter[value_column].max()
                except Exception as e:
                    # Log any errors that occur during the calculation
                    system.log_error(e)

            # Extract the indices and values for each quarter
            i3, v3 = quarter_indices['March'], quarter_values['March']
            i6, v6 = quarter_indices['June'], quarter_values['June']
            i9, v9 = quarter_indices['September'], quarter_values['September']
            i12, v12 = quarter_indices['December'], quarter_values['December']

            # Nested function to apply B3-specific math logic based on the 'conta' prefix
            def apply_math_logic(v3, v6, v9, v12):
                try:
                    # Adjust the values for the last quarter or all quarters depending on the prefix
                    if conta_prefix in settings.last_quarters:
                        v12 -= (v9 + v6 + v3)
                    elif conta_prefix in settings.all_quarters:
                        v6 -= v3
                        v9 -= (v6 + v3)
                        v12 -= (v9 + v6 + v3)
                except Exception as e:
                    system.log_error(e)
                return v3, v6, v9, v12

            # Apply the logic to update the values
            v3, v6, v9, v12 = apply_math_logic(v3, v6, v9, v12)

            # Nested function to update the DataFrame with the new calculated values
            def update_quarter_values():
                for quarter_name, idx in quarter_indices.items():
                    if idx is not None:
                        df_group.loc[idx, value_column] = quarter_values[quarter_name]

            # Update the DataFrame with the new values
            update_quarter_values()

        # Return the updated DataFrame
        return df_group
    except Exception as e:
        # Log any errors that occur during the calculation
        system.log_error(e)
        return df_group

def save_db(df, db_file_path):
    """
    Save the DataFrame to the specified database file.

    Parameters:
    - dataframe (DataFrame): The DataFrame to save.
    - db_file_path (str): Path to the database file.
    """
    try:
        # Create a connection to the database
        conn = sqlite3.connect(db_file_path)
        
        # Save the DataFrame to the database, appending to the existing table or replacing it
        df.to_sql('finsheet', conn, if_exists='append', index=False)
        
        # Close the database connection
        conn.close()
        
        return df

    except Exception as e:
        system.log_error(e)

def main():
    """
    Main function to load existing math.db, process new database files, 
    and save only new data based on comparison with existing data.
    """
    try:
        # Hard-coded variables
        base_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_folder = settings.db_folder_short
        db_name = settings.db_name
        backup_keyword = 'backup'
        math_keyword = 'math'

        # Construct paths and file names
        database_folder = os.path.join(base_directory, db_folder)
        base_name, extension = os.path.splitext(db_name)
        database_prefix = f"{base_name} "

        # Find all database files in the specified folder that match the prefix
        database_files = glob.glob(os.path.join(database_folder, f'{database_prefix}*.db'))
        
        # Filter out any backup or math database files
        valid_database_files = [db_file for db_file in database_files if backup_keyword not in db_file and math_keyword not in db_file]
        
        # List to accumulate all processed math data
        all_processed_math_data = []
        
        # Track the total number of files and the start time for processing
        total_files = len(valid_database_files)
        start_time = time.time()

        # Iterate over each valid database file and process it
        for file_index, db_file_path in enumerate(valid_database_files):
            # Print progress information
            system.print_info(file_index, total_files, total_files, extra_info=[db_file_path], start_time=start_time, size=total_files)
            
            # Process the database and save the transformed data
            df_processed_math = process_database(db_file_path)
 
            if not df_processed_math.empty:
                all_processed_math_data.append(df_processed_math)

        # Concatenate all non-empty processed DataFrames into a single DataFrame
        if all_processed_math_data:
            final_concatenated_data = pd.concat(all_processed_math_data).reset_index(drop=True)
        else:
            final_concatenated_data = pd.DataFrame()  # Empty DataFrame if no data was processed

        # The concatenated processed math data
        return final_concatenated_data

    except Exception as e:
        # Log any errors that occur during the main processing
        system.log_error(e)

# Run the main function when the script is executed
if __name__ == "__main__":
    main()
