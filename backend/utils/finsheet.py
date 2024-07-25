# finsheet.py

import pandas as pd
import sqlite3
from utils import system
from utils import selenium_driver
from config import settings

def get_nsd_list(criteria=settings.finsheet_types):
    """
    Retrieve NSD values based on criteria and perform an outer merge with company_info table.
    
    Parameters:
    - criteria (list): List of nsd_type criteria to filter NSD values.

    Returns:
    list: A list of NSD values ordered by setor, subsetor, segmento, company, quarter, and version.
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(f'{settings.db_folder}/{settings.db_name}')
        
        # Prepare the criteria string for SQL query
        criteria_str = ', '.join(f"'{c}'" for c in criteria)
        
        # Retrieve NSD values based on criteria
        query_nsd = f"""
        SELECT nsd, company, version, quarter 
        FROM nsd 
        WHERE nsd_type IN ({criteria_str})
        """
        df_nsd = pd.read_sql_query(query_nsd, conn)
        
        # Ensure correct data types
        df_nsd['nsd'] = df_nsd['nsd'].astype(str)
        df_nsd['version'] = df_nsd['version'].astype(str)
        
        # Retrieve company info
        query_company = "SELECT company_name, cvm_code, setor, subsetor, segmento FROM company_info"
        df_company = pd.read_sql_query(query_company, conn)
        
        # Ensure correct data types
        df_company['cvm_code'] = df_company['cvm_code'].astype(str)
        
        # Perform outer merge and fill NaN values
        df_merged = pd.merge(df_nsd, df_company, how='outer', left_on='company', right_on='company_name')
        df_merged = df_merged.fillna('')
        
        # Drop older versions, keeping only the latest version for each company-quarter combination
        df_merged = df_merged.sort_values(by=['company', 'quarter', 'version'], ascending=[True, True, True])
        df_merged = df_merged.drop_duplicates(subset=['company', 'quarter'], keep='last') # comment to keep duplicates quarters (different versions)
        
        # Custom sorting to place empty fields last
        last_order = 'ZZZZZZZZZZ'
        df_merged.loc[df_merged['setor'] == '', 'setor'] = last_order
        df_merged.loc[df_merged['subsetor'] == '', 'subsetor'] = last_order
        df_merged.loc[df_merged['segmento'] == '', 'segmento'] = last_order
        
        # Order the list by setor, subsetor, segmento, company, quarter, and version
        df_sorted = df_merged.sort_values(by=['setor', 'subsetor', 'segmento', 'company', 'quarter', 'version'])
        
        # Restore empty fields
        df_sorted.loc[df_sorted['setor'] == last_order, 'setor'] = ''
        df_sorted.loc[df_sorted['subsetor'] == last_order, 'subsetor'] = ''
        df_sorted.loc[df_sorted['segmento'] == last_order, 'segmento'] = ''
        
        # Close the connection
        conn.close()
        return df_sorted['nsd'].tolist()
    
    except Exception as e:
        system.log_error(e)
        return []

def main(finsheet=None):
    
    nsd_list = get_nsd_list(settings.finsheet_types)

    return finsheet

if __name__ == "__main__":
    finsheet = main()
    print('done')
