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


def log_error(e: Exception) -> None:
    """
    Logs an error with the current function name.

    Parameters:
    - e (Exception): The exception to log.
    """
    # Get the name of the function where the error occurred and log the error
    print(f'Error in {inspect.currentframe().f_back.f_code.co_name}: {e}')


def clean_text(text: str) -> str:
    """
    Cleans and normalizes the input text by removing punctuation, converting to uppercase, and stripping whitespace.

    Parameters:
    - text (str): The input text to be cleaned.

    Returns:
    str: The cleaned and normalized text.
    """
    try:
        # Remove accents, punctuation, convert to uppercase, and strip leading/trailing whitespace
        text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()

        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)
    except Exception as e:
        # Log any error that occurs during text cleaning
        log_error(e)
    
    return text


def text(xpath: str, driver_wait: object) -> str:
    """
    Finds and retrieves text from a web element using the provided xpath and wait object.

    Parameters:
    - xpath (str): The xpath of the element to retrieve text from.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.

    Returns:
    str: The text of the element, or an empty string if an exception occurs.
    """
    try:
        # Wait for the element to be present, then retrieve its text
        element = wait_forever(driver_wait, xpath)
        return element.text
    except Exception as e:
        # Log any error that occurs while retrieving the text
        log_error(e)
    
        return ''


def click(xpath: str, driver_wait: object) -> bool:
    """
    Finds and clicks on a web element using the provided xpath and wait object.

    Parameters:
    - xpath (str): The xpath of the element to click.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.

    Returns:
    bool: True if the element was found and clicked, False otherwise.
    """
    try:
        # Wait for the element to be clickable, then click it
        element = wait_forever(driver_wait, xpath)
        element.click()
        return True
    except Exception as e:
        # Log any error that occurs while clicking the element
        log_error(e)
    
        return False


def choose(xpath: str, driver: object, driver_wait: object) -> int:
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
        # Wait for the element to be present, then click it to reveal options
        element = wait_forever(driver_wait, xpath)
        element.click()

        # Get the select dropdown and extract all option values as integers
        select = Select(driver.find_element(By.XPATH, xpath))
        options = [int(x.text) for x in select.options]

        # Select the maximum value in the dropdown
        batch = str(max(options))
        select.select_by_value(batch)

        return int(batch)
    except Exception as e:
        # Log any error that occurs during selection
        log_error(e)
    
        return ''


def send_keys(xpath: str, driver: object, driver_wait: object, keyword: str) -> object:
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
        # Wait for the element to be ready for input, then send the keyword to it
        element = wait_forever(driver_wait, xpath)
        element.send_keys(keyword)
        return element
    except Exception as e:
        # Log any error that occurs while sending keys
        log_error(e)
    
        return None


def wait_forever(driver_wait: object, xpath: str) -> object:
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
            # Wait until the element is present in the DOM and return it
            element = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            return element
        except Exception:
            # Sleep for a while and retry, useful when the element may take time to appear
            time.sleep(settings.wait_time)


def link(xpath: str, driver_wait: object) -> str:
    """
    Finds and retrieves the href attribute of a web element using the provided xpath and wait object.

    Parameters:
    - xpath (str): The xpath of the web element.
    - driver_wait (WebDriverWait): The wait object to use.

    Returns:
    str: The href attribute of the web element, or an empty string if an exception occurs.
    """
    try:
        # Wait for the element to be present, then retrieve its href attribute
        element = wait_forever(driver_wait, xpath)
        return element.get_attribute('href')
    except Exception as e:
        # Log any error that occurs while retrieving the href attribute
        log_error(e)
    
        return ''


def raw_text(xpath: str, driver_wait: object) -> str:
    """
    Finds and retrieves the raw HTML text from a web element using the provided xpath and wait object.

    Parameters:
    - xpath (str): The xpath of the element to retrieve text from.
    - driver_wait (WebDriverWait): The wait object to use for finding the element.

    Returns:
    str: The raw HTML text of the element, or an empty string if an exception occurs.
    """
    try:
        # Wait for the element to be present, then retrieve its innerHTML
        element = wait_forever(driver_wait, xpath)
        return element.get_attribute('innerHTML')
    except Exception as e:
        # Log any error that occurs while retrieving the raw HTML
        log_error(e)
    
        return ''


def winbeep(frequency: int = 5000, duration: int = 50) -> bool:
    """
    Generates a system beep sound with the specified frequency and duration.

    Parameters:
    - frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
    - duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

    Returns:
    bool: True if the beep was successful, False otherwise.
    """
    # Use the winsound module to generate a beep sound
    winsound.Beep(frequency, duration)
    return True


def print_info(current_index: int, total_items: int, extra_info: list, start_time: float) -> None:
    """
    Prints the progress information along with the remaining time.

    Parameters:
    - current_index (int): The current item index (0-based).
    - total_items (int): The total number of items to process.
    - extra_info (list): Additional information to be printed.
    - start_time (float): The start time of the process (timestamp).
    """

    # Calculate how many items have been processed so far.
    processed_items = current_index + 1

    # Determine how many items are left to process.
    remaining_items = total_items - processed_items

    # Calculate the percentage of completion by dividing processed items by total items.
    percentage_complete = processed_items / total_items

    # Compute the total time elapsed since the process started.
    elapsed_time = time.time() - start_time

    # Calculate the average time taken to process one item.
    avg_time_per_item = elapsed_time / processed_items

    # Estimate the remaining time by multiplying the average time per item by the number of remaining items.
    remaining_time = remaining_items * avg_time_per_item

    # Convert the remaining time from seconds to hours, minutes, and seconds.
    hours, remainder = divmod(int(remaining_time), 3600)  # `divmod` gives quotient (hours) and remainder (seconds)
    minutes, seconds = divmod(remainder, 60)  # Further divide remainder into minutes and seconds

    # Format the remaining time into a string like 'Xh Ym Zs'.
    remaining_time_formatted = f'{int(hours)}h {int(minutes):02}m {int(seconds):02}s'

    # Create a progress string that shows percentage complete, items processed, and remaining time.
    progress = (
        f'{percentage_complete:.2%} '  # Format the percentage as a string with two decimal places
        f'{processed_items}/{total_items}, '  # Show how many items have been processed out of the total
        f'{avg_time_per_item:.6f}s per item, '  # Show average processing time per item
        f'Remaining: {remaining_time_formatted}'  # Show the remaining time in a human-readable format
    )

    # Convert the extra information list into a single string, joining elements with spaces.
    extra_info_str = ' '.join(map(str, extra_info))

    # Print the progress information followed by the extra information.
    print(f'{progress} {extra_info_str}')

    # Emit a beep sound to indicate an update (useful for long-running processes).
    winbeep()


def get_db_schema(db_name: str) -> dict:
    """
    Retrieve schema information for all objects (tables, indexes, views, triggers) in the SQLite database.

    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    dict: A dictionary containing schema information for tables, indexes, views, and triggers.
    """
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(f'{settings.db_folder}/{db_name}')
    cursor = conn.cursor()

    def get_table_schema(table_name: str) -> pd.DataFrame:
        # Query the schema for a specific table
        cursor.execute(f'PRAGMA table_info({table_name});')
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Data Type', 'Not Null', 'Default Value', 'Primary Key'])

    def get_index_schema(index_name: str) -> pd.DataFrame:
        # Query the schema for a specific index
        cursor.execute(f'PRAGMA index_info({index_name});')
        schema = cursor.fetchall()
        return pd.DataFrame(schema, columns=['Column ID', 'Column Name', 'Sort Order'])

    def get_view_schema(view_name: str) -> pd.DataFrame:
        # Query the SQL definition for a specific view
        cursor.execute(f'SELECT sql FROM sqlite_master WHERE type="view" AND name="{view_name}";')
        schema = cursor.fetchone()
        return pd.DataFrame([schema], columns=['View SQL'])

    def get_trigger_schema(trigger_name: str) -> pd.DataFrame:
        # Query the SQL definition for a specific trigger
        cursor.execute(f'SELECT sql FROM sqlite_master WHERE type="trigger" AND name="{trigger_name}";')
        schema = cursor.fetchone()
        return pd.DataFrame([schema], columns=['Trigger SQL'])

    # Initialize a dictionary to hold schema information
    schema_dict = {'tables': {}, 'indexes': {}, 'views': {}, 'triggers': {}}

    # Retrieve and store schema information for all tables in the database
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table";')
    tables = cursor.fetchall()
    for table in [t[0] for t in tables]:
        schema_dict['tables'][table] = get_table_schema(table)

    # Retrieve and store schema information for all indexes in the database
    cursor.execute('SELECT name FROM sqlite_master WHERE type="index";')
    indexes = cursor.fetchall()
    for index in [i[0] for i in indexes]:
        schema_dict['indexes'][index] = get_index_schema(index)

    # Retrieve and store schema information for all views in the database
    cursor.execute('SELECT name FROM sqlite_master WHERE type="view";')
    views = cursor.fetchall()
    for view in [v[0] for v in views]:
        schema_dict['views'][view] = get_view_schema(view)

    # Retrieve and store schema information for all triggers in the database
    cursor.execute('SELECT name FROM sqlite_master WHERE type="trigger";')
    triggers = cursor.fetchall()
    for trigger in [t[0] for t in triggers]:
        schema_dict['triggers'][trigger] = get_trigger_schema(trigger)

    # Close the database connection
    conn.close()

    return schema_dict


def load_database(db_name: str) -> dict:
    """
    Load each table from the SQLite database into its own DataFrame.

    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    dict: A dictionary where each key is a table name and each value is a DataFrame containing the table's data.
    """
    # Establish a connection to the SQLite database
    conn = sqlite3.connect(f'{settings.db_folder}/{db_name}')

    def get_table_names() -> list:
        # Retrieve the names of all tables in the database
        cursor = conn.cursor()
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table";')
        tables = cursor.fetchall()
        return [table[0] for table in tables]

    # Initialize a dictionary to hold DataFrames for each table
    table_dfs = {}

    # Retrieve the list of table names
    table_names = get_table_names()

    # Load each table into its own DataFrame
    for table_name in table_names:
        query = f'SELECT * FROM {table_name};'
        table_dfs[table_name] = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return table_dfs
