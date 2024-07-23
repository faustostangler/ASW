import inspect
import winsound
import time
import string
import unidecode
import re
import sqlite3
import pandas as pd


def log_error(e):
    """
    Logs an error with the current function name.
    
    Parameters:
    - e (Exception): The exception to log.
    """
    print(f"Error in {inspect.currentframe().f_back.f_code.co_name}: {e}")

def clean_text(text):
    try:
        text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()
        text = re.sub(r'\s+', ' ', text)
    except Exception as e:
        log_error(e)
    return text

def winbeep(frequency=5000, duration=50):
    """
    Generate a system beep sound with the specified frequency and duration.

    Args:
        frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
        duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

    Returns:
        bool: True if the beep was successful, False otherwise.
    """
    winsound.Beep(frequency, duration)
    return True

def print_info(i, start, end, data, start_time, size):
    """
    Prints the provided information along with progress and remaining time.

    Parameters:
    - i (int): The current nsd value.
    - start (int): The start value of the current batch.
    - end (int): The end value of the current batch.
    - data (tuple): The extracted data tuple containing multiple values.
    - start_time (float): The start time of the process.
    - size (int): The total number of items to process.
    """
    # Calculate remaining time and progress
    counter = i + 1
    remaining_items = size - counter
    
    # Calculate the percentage of completion
    percentage = counter / size
    
    # Calculate the elapsed time
    running_time = time.time() - start_time
    
    # Calculate the average time taken per item
    avg_time_per_item = running_time / counter
    
    # Calculate the remaining time based on the average time per item
    remaining_time = remaining_items * avg_time_per_item
    
    # Convert remaining time to hours, minutes, and seconds
    hours, remainder = divmod(int(remaining_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Format remaining time as a string
    remaining_time_formatted = f'{int(hours)}h {int(minutes):02}m {int(seconds):02}s'
    
    # Create a progress string with all the calculated values
    progress = (
        f'{percentage:.2%} '
        f'{counter}+{remaining_items}, '
        f'{avg_time_per_item:.6f}s per item, '
        f'Remaining: {remaining_time_formatted} '
    )
    
    # Print the information
    extra_info = " ".join(map(str, data))
    print(f"{progress} {extra_info}")
    
    winbeep()

def get_db_schema(db_name):
    """Retrieve schema information for all objects (tables, indexes, views, triggers) in the SQLite database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Define a function to get schema information for tables
    def get_table_schema(table_name):
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Data Type', 'Not Null', 'Default Value', 'Primary Key'])
    
    # Define a function to get schema information for indexes
    def get_index_schema(index_name):
        cursor.execute(f"PRAGMA index_info({index_name});")
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Sort Order'])
    
    # Define a function to get schema information for views
    def get_view_schema(view_name):
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='view' AND name='{view_name}';")
        schema = cursor.fetchone()
        return pd.DataFrame([schema], columns=['View SQL'])
    
    # Define a function to get schema information for triggers
    def get_trigger_schema(trigger_name):
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='trigger' AND name='{trigger_name}';")
        schema = cursor.fetchone()
        return pd.DataFrame([schema], columns=['Trigger SQL'])
    
    # Initialize the dictionary to hold schema information
    schema_dict = {'tables': {}, 'indexes': {}, 'views': {}, 'triggers': {}}
    
    # Get and store schema information for tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in [t[0] for t in tables]:
        schema_dict['tables'][table] = get_table_schema(table)
    
    # Get and store schema information for indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
    indexes = cursor.fetchall()
    for index in [i[0] for i in indexes]:
        schema_dict['indexes'][index] = get_index_schema(index)
    
    # Get and store schema information for views
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
    views = cursor.fetchall()
    for view in [v[0] for v in views]:
        schema_dict['views'][view] = get_view_schema(view)
    
    # Get and store schema information for triggers
    cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger';")
    triggers = cursor.fetchall()
    for trigger in [t[0] for t in triggers]:
        schema_dict['triggers'][trigger] = get_trigger_schema(trigger)
    
    conn.close()
    
    # Return the complete dictionary with all object types
    return schema_dict

def load_database(db_name):
    """Load each table from the SQLite database into its own DataFrame."""
    conn = sqlite3.connect(db_name)
    
    # Define a function to get table names
    def get_table_names():
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    
    # Initialize a dictionary to hold DataFrames for each table
    table_dfs = {}
    
    # Get the list of table names
    table_names = get_table_names()
    
    # Load each table into its own DataFrame
    for table_name in table_names:
        query = f"SELECT * FROM {table_name};"
        table_dfs[table_name] = pd.read_sql_query(query, conn)
    
    conn.close()
    
    return table_dfs

