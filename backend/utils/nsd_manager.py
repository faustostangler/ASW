import sqlite3
import pandas as pd

from utils import system
from config import settings

def load_all_nsds(db_name):
    """
    Loads all NSD numbers from the database.
    
    Parameters:
    - db_name (str): The name of the database file.
    
    Returns:
    list: A list of NSD numbers.
    """
    try:
        conn = sqlite3.connect(db_name)
        query = "SELECT nsd FROM nsd"
        nsd_df = pd.read_sql_query(query, conn)
        conn.close()
        return nsd_df['nsd'].tolist()
    except Exception as e:
        system.log_error(e)
        return []

def generate_missing_nsds(all_nsds):
    """
    Generates a list of negative NSD numbers that are not present in the provided list.
    
    Parameters:
    - all_nsds (list): A list of NSD numbers.
    
    Returns:
    list: A list of negative NSD numbers.
    """
    try:
        max_nsd = max(all_nsds)
        full_set = set(range(1, max_nsd + 1))
        existing_set = set(all_nsds)
        missing_nsds = sorted(list(full_set - existing_set))
        return missing_nsds
    except Exception as e:
        system.log_error(e)
        return []

def manage_nsd(db_name = 'b3.db'):
    all_nsds = load_all_nsds(db_name)
    negative_nsds = generate_missing_nsds(all_nsds)

# Example usage
if __name__ == "__main__":
    try:
        db_name = 'b3.db'
        manage_nsd(db_name)
        print(f"Negative NSD numbers: {missing_nsds}")
    except Exception as e:
        system.log_error(e)
