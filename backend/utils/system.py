import inspect
import winsound
import time
import string
import unidecode
import re
import sqlite3
import pandas as pd
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config import settings

def log_error(e):
    """
    Logs an error with the current function name.
    
    Parameters:
    - e (Exception): The exception to log.
    """
    print(f"Error in {inspect.currentframe().f_back.f_code.co_name}: {e}")

def clean_text(text):
    """
    Cleans and normalizes the input text by removing punctuation, converting to uppercase, and stripping whitespace.
    
    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    str: The cleaned and normalized text.
    """
    try:
        text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()
        text = re.sub(r'\s+', ' ', text)
    except Exception as e:
        log_error(e)
    return text

def text(xpath, driver_wait):
    """
    Finds and retrieves text from a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the element to retrieve text from.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    str: The text of the element, or an empty string if an exception occurs.
    """
    try:
        # Wait until the element is present, then retrieve its text.
        element = wait_forever(driver_wait, xpath)
        text = element.text
        return text
    except Exception as e:
        log_error(e)
        return ''

def click(xpath, driver_wait):
    """
    Finds and clicks on a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the element to click.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    bool: True if the element was found and clicked, False otherwise.
    """
    try:
        # Wait until the element is clickable, then click it.
        element = wait_forever(driver_wait, xpath)
        element.click()
        return True
    except Exception as e:
        log_error(e)
        return False

def choose(xpath, driver, driver_wait):
    """
    Finds and selects a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the element to select.
    - driver (webdriver.Chrome): The Chrome driver object to use for selecting the element.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    int: The value of the selected option, or an empty string if an exception occurs.
    """
    try:
        element = wait_forever(driver_wait, xpath)
        element.click()
        
        # Get the Select object for the element, find the maximum option value, and select it.
        select = Select(driver.find_element(By.XPATH, xpath))
        options = [int(x.text) for x in select.options]
        batch = str(max(options))
        select.select_by_value(batch)
        
        return int(batch)
    except Exception as e:
        log_error(e)
        return ''
   
def send_keys(xpath, driver, driver_wait, keyword):
    """
    Finds and sends keys to a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the element to send keys to.
    - driver (webdriver.Chrome): The Chrome driver object to use.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.
    - keyword (str): The keyword to send to the element.
    
    Returns:
    WebElement: The web element after sending the keys, or None if an exception occurs.
    """
    try:
        # Wait until the element is clickable, then send the keyword to it.
        element = wait_forever(driver_wait, xpath)
        element.send_keys(keyword)
        return element
    except Exception as e:
        log_error(e)
        return None

def wait_forever(driver_wait, xpath):
    """
    Waits indefinitely until the web element located by the given xpath is found.
    
    Parameters:
    - driver_wait (WebDriverWait): The wait object to use.
    - xpath (str): The xpath of the element to wait for.
    
    Returns:
    WebElement: The found web element.
    """
    while True:
        try:
            element = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            break
        except Exception as e:
            log_error(e)
            time.sleep(settings.wait_time)  # Wait for a specified time and try again
    return element

def link(xpath, driver_wait):
    """
    Finds and retrieves the href attribute of a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the web element.
    - driver_wait (WebDriverWait): The wait object to use.
    
    Returns:
    str: The href attribute of the web element, or an empty string if an exception occurs.
    """
    try:
        element = wait_forever(driver_wait, xpath)
        href = element.get_attribute('href')
        return href
    except Exception as e:
        log_error(e)
        return ''

def raw_text(xpath, driver_wait):
    """
    Finds and retrieves the raw HTML text from a web element using the provided xpath and wait object.
    
    Parameters:
    - xpath (str): The xpath of the element to retrieve text from.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.
    
    Returns:
    str: The raw HTML text of the element, or an empty string if an exception occurs.
    """
    try:
        # Wait until the element is present, then retrieve its innerHTML.
        element = wait_forever(driver_wait, xpath)
        raw_code = element.get_attribute("innerHTML")
        return raw_code
    except Exception as e:
        log_error(e)
        return ''

def winbeep(frequency=5000, duration=50):
    """
    Generates a system beep sound with the specified frequency and duration.

    Parameters:
    - frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
    - duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

    Returns:
    bool: True if the beep was successful, False otherwise.
    """
    winsound.Beep(frequency, duration)
    return True

def print_info(i, start, end, extra_info, start_time, size):
    """
    Prints the provided information along with progress and remaining time.

    Parameters:
    - i (int): The current item index.
    - start (int): The start value of the current batch.
    - end (int): The end value of the current batch.
    - extra_info (list): The extracted extra information containing multiple values.
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
        f'Remaining: {remaining_time_formatted}'
    )
    
    # Print the information
    extra_info = " ".join(map(str, extra_info))
    print(f"{progress} {extra_info}")
    
    winbeep()

def get_db_schema(db_name):
    """
    Retrieve schema information for all objects (tables, indexes, views, triggers) in the SQLite database.
    
    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    dict: A dictionary containing schema information for tables, indexes, views, and triggers.
    """
    conn = sqlite3.connect(f'{settings.database_folder}/{db_name}')
    cursor = conn.cursor()
    
    def get_table_schema(table_name):
        """
        Retrieves schema information for a table.
        
        Parameters:
        - table_name (str): The name of the table.

        Returns:
        DataFrame: A DataFrame containing the schema information of the table.
        """
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Data Type', 'Not Null', 'Default Value', 'Primary Key'])
    
    def get_index_schema(index_name):
        """
        Retrieves schema information for an index.
        
        Parameters:
        - index_name (str): The name of the index.

        Returns:
        DataFrame: A DataFrame containing the schema information of the index.
        """
        cursor.execute(f"PRAGMA index_info({index_name});")
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Sort Order'])
    
    def get_view_schema(view_name):
        """
        Retrieves schema information for a view.
        
        Parameters:
        - view_name (str): The name of the view.

        Returns:
        DataFrame: A DataFrame containing the SQL of the view.
        """
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='view' AND name='{view_name}';")
        schema = cursor.fetchone()
        return pd.DataFrame([schema], columns=['View SQL'])
    
    def get_trigger_schema(trigger_name):
        """
        Retrieves schema information for a trigger.
        
        Parameters:
        - trigger_name (str): The name of the trigger.

        Returns:
        DataFrame: A DataFrame containing the SQL of the trigger.
        """
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
    """
    Load each table from the SQLite database into its own DataFrame.
    
    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    dict: A dictionary where each key is a table name and each value is a DataFrame containing the table's data.
    """
    conn = sqlite3.connect(f'{settings.database_folder}/{db_name}')
    
    def get_table_names():
        """
        Retrieves the names of all tables in the database.
        
        Returns:
        list: A list of table names.
        """
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