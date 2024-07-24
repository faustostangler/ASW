import sqlite3
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

from utils import system
from config import settings

def get_nsds_from_db(db_name, nsd_type):
    """
    Retrieves NSD numbers from the database that match the given nsd_type.
    
    Parameters:
    - db_name (str): The name of the database file.
    - nsd_type (str): The NSD type to filter.
    
    Returns:
    list: A list of NSD numbers.
    """
    try:
        conn = sqlite3.connect(f'{settings.database_folder}/{db_name}')
        query = f"SELECT nsd FROM nsd"
        nsd_df = pd.read_sql_query(query, conn)
        conn.close()
        return nsd_df['nsd'].tolist()
    except Exception as e:
        system.log_error(e)
        return []

def scrape_financial_table(driver, driver_wait, nsd):
    """
    Scrapes the financial table from the iframe within the given NSD page.
    
    Parameters:
    - driver (webdriver.Chrome): The Selenium WebDriver instance.
    - wait (WebDriverWait): The WebDriverWait instance.
    - nsd (int): The NSD number to scrape.
    
    Returns:
    pd.DataFrame: A DataFrame containing the scraped table data.
    """
    url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
    driver.get(url)
    
    try:
        # Switch to iframe
        iframe = driver_wait.until(EC.presence_of_element_located((By.TAG_NAME, 'iframe')))
        driver.switch_to.frame(iframe)
        
        # Locate the table
        xpath = '//*[@id="ctl00_cphPopUp_tbDados"]'
        table = system.wait_forever(driver_wait, xpath)
        rows = table.find_elements(By.TAG_NAME, 'tr')
        
        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            data.append([col.text for col in cols])
        
        driver.switch_to.default_content()  # Switch back to default content
        
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        return df
    except (NoSuchElementException, TimeoutException) as e:
        print(f"Error scraping NSD {nsd}: {e}")
        return pd.DataFrame()

def main_scrape_process(driver, driver_wait):
    """
    Main process to scrape financial tables for the given NSD type and save new data.
    
    Parameters:
    - driver (webdriver.Chrome): The Selenium WebDriver instance.
    - wait (WebDriverWait): The WebDriverWait instance.
    - db_name (str): The name of the database file.
    - nsd_type (str): The NSD type to filter.
    """
    try:
        db_name=settings.db_name
        nsd_type='INFORMACOES TRIMESTRAIS'

        existing_nsds = get_nsds_from_db(db_name, nsd_type)
        all_nsds = set(existing_nsds)
        
        # Fetch new NSDs from some source, here assumed to be fetched previously
        nsd.nsd_scrape(driver, driver_wait)
        new_nsds = []  # Replace with actual method to fetch new NSDs
        
        for nsd in new_nsds:
            if nsd not in all_nsds:
                df = scrape_financial_table(driver, driver_wait, nsd)
                if not df.empty:
                    print(f"Scraped data for NSD {nsd}:")
                    print(df.head())
                else:
                    print(f"No data found for NSD {nsd}.")
    except Exception as e:
        print(f"Error in main_scrape_process: {e}")
