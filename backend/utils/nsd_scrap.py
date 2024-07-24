import time
import sqlite3
import pandas as pd
import os
import shutil
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
from datetime import datetime, timedelta
from utils import system
from utils import selenium_driver
from config import settings

def parse_data(driver, driver_wait, i):
    """
    Parses the data from the web page for the given NSD number.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - i (int): The NSD number.

    Returns:
    dict: A dictionary containing the parsed data.
    """
    data = {}
    try:
        data['nsd'] = i

        # Extract company name
        xpath = '//*[@id="lblNomeCompanhia"]'
        company_element = system.wait_forever(driver_wait, xpath)
        company_text = system.clean_text(company_element.text)
        data['company'] = re.sub(settings.words_to_remove, '', company_text)

        # Extract DRI name
        dri_element = driver.find_element(By.XPATH, '//*[@id="lblNomeDRI"]')
        data['dri'] = system.clean_text(dri_element.text.split('-')[0].strip())

        # Extract NSD type and version
        nsd_type_element = driver.find_element(By.XPATH, '//*[@id="lblDescricaoCategoria"]')
        nsd_type_version = nsd_type_element.text
        data['nsd_type'] = system.clean_text(nsd_type_version.split('-')[0].strip())
        data['version'] = int(re.search(r'V(\d+)', nsd_type_version).group(1))

        # Extract auditor name
        auditor_element = driver.find_element(By.XPATH, '//*[@id="lblAuditor"]')
        data['auditor'] = system.clean_text(auditor_element.text.split('-')[0].strip())

        # Extract auditor responsible
        auditor_rt_element = driver.find_element(By.XPATH, '//*[@id="lblResponsavelTecnico"]')
        data['auditor_rt'] = system.clean_text(auditor_rt_element.text)

        # Extract protocol number
        protocolo_element = driver.find_element(By.XPATH, '//*[@id="lblProtocolo"]')
        data['protocolo'] = protocolo_element.text.replace('-', '').strip()

        # Extract document date
        date_element = driver.find_element(By.XPATH, '//*[@id="lblDataDocumento"]')
        date_str = date_element.text
        data['date'] = pd.to_datetime(date_str, dayfirst=True, errors='coerce')

        # Extract sent date
        sent_date_element = driver.find_element(By.XPATH, '//*[@id="lblDataEnvio"]')
        sent_date_str = sent_date_element.text
        data['sent_date'] = pd.to_datetime(sent_date_str, dayfirst=True, errors='coerce')

        # Extract reason for cancellation or re-presentation
        reason_element = driver.find_element(By.XPATH, '//*[@id="lblMotivoCancelamentoReapresentacao"]')
        data['reason'] = system.clean_text(reason_element.text)
    except Exception as e:
        system.log_error(e)
        pass

    return data if 'sent_date' in data else None

def save_to_db(data, db_name=settings.db_name):
    """
    Saves the parsed data to the database.

    Parameters:
    - data (list): A list of dictionaries containing parsed data.
    - db_name (str): Name of the database file.
    """
    try:
        if not data:
            return

        # Backup existing database
        base_name, ext = os.path.splitext(db_name)
        backup_name = f"{base_name} backup{ext}"
        db_path = os.path.join(settings.database_folder, db_name)
        backup_path = os.path.join(settings.database_folder, backup_name)
        shutil.copy2(db_path, backup_path)

        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS nsd
                        (nsd INTEGER PRIMARY KEY, company TEXT, dri TEXT, nsd_type TEXT, version INTEGER, auditor TEXT,
                        auditor_rt TEXT, protocolo TEXT, quarter TEXT, sent_date TEXT, reason TEXT)''')

        for item in data:
            quarter_str = item['date'].isoformat() if item['date'] else None
            sent_date_str = item['sent_date'].isoformat() if item['sent_date'] else None

            cursor.execute('''INSERT INTO nsd 
                            (nsd, company, dri, nsd_type, version, auditor, auditor_rt, protocolo, quarter, sent_date, reason) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(nsd) DO UPDATE SET
                            company=excluded.company,
                            dri=excluded.dri,
                            nsd_type=excluded.nsd_type,
                            version=excluded.version,
                            auditor=excluded.auditor,
                            auditor_rt=excluded.auditor_rt,
                            protocolo=excluded.protocolo,
                            quarter=excluded.quarter,
                            sent_date=excluded.sent_date,
                            reason=excluded.reason''',
                            (item['nsd'], item['company'], item['dri'], item['nsd_type'], item['version'], 
                            item['auditor'], item['auditor_rt'], item['protocolo'], 
                            quarter_str, sent_date_str, item['reason']))

        conn.commit()
        conn.close()

        print('Partial save completed...')
    except Exception as e:
        system.log_error(e)

def generate_nsd_list(db_name):
    """
    Generates a list of new NSD numbers to scrape and finds missing NSD values.

    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    tuple: A tuple with two elements:
           - A list of new NSD numbers generated based on date difference.
           - A list of missing NSD values from the database.
    """
    conn = sqlite3.connect(f'{settings.database_folder}/{db_name}')
    cursor = conn.cursor()

    # Fetch the NSD data
    cursor.execute('SELECT nsd, MIN(sent_date), MAX(sent_date) FROM nsd')
    result = cursor.fetchone()
    
    # Retrieve all existing NSD values
    cursor.execute("SELECT nsd FROM nsd ORDER BY nsd;")
    existing_nsds = cursor.fetchall()
    
    # Close the database connection
    conn.close()
    
    # Convert list of tuples to a list of integers
    existing_nsds = [nsd[0] for nsd in existing_nsds]
    
    if not existing_nsds:
        return ([], [])  # Return tuple with empty lists if no NSD values are present
    
    # Determine the maximum NSD value
    max_nsd = max(existing_nsds)
    
    # Generate new NSD list based on date difference
    if result and result[0]:
        last_nsd = result[0]
        first_date = pd.to_datetime(result[1])
        last_date = pd.to_datetime(result[2])

        days_diff_total = (last_date - first_date).days
        items_per_day = last_nsd / days_diff_total if days_diff_total != 0 else 0

        current_date = pd.to_datetime(datetime.now())
        days_to_current = (current_date - last_date).days
        remaining_items = int(items_per_day * days_to_current)

        nsd_new_values = list(range(last_nsd, last_nsd + remaining_items + 1))
    
    else:
        nsd_new_values = []
    
    # Create a set of all possible NSD values in the range 1 to max_nsd
    full_set = set(range(1, max_nsd + 1))
    existing_set = set(existing_nsds)
    
    # Find missing NSD values
    nsd_missing_values = sorted(full_set - existing_set)
    
    return nsd_new_values, nsd_missing_values

def nsd_scrape(driver, driver_wait, nsd_list):
    """
    Scrapes NSD data for each NSD number in nsd_list.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - nsd_list (list): A list of NSD numbers to scrape.
    """
    try:
        db_name = settings.db_name
        all_data = []
        size = len(nsd_list)
        start_time = time.time()

        for i, nsd in enumerate(nsd_list):
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            driver.get(url)

            data = parse_data(driver, driver_wait, nsd)

            if data:
                all_data.append(data)
                extra_info = [nsd, data['sent_date'], data['date'].strftime('%Y-%m'), data['nsd_type'], data['company']]
            else:
                extra_info = []

            system.print_info(i, nsd_list[0], nsd_list[-1], extra_info, start_time, size)

            if (i + 1) % settings.batch_size == 0 or i == size - 1:
                save_to_db(all_data, db_name)
                all_data.clear()

        save_to_db(all_data, db_name)
        print('Final save completed...')

    except Exception as e:
        system.log_error(e)

def scrape_nsd_values(driver, driver_wait, db_name=settings.db_name):
    """
    Scrapes NSD values from the website and saves them to the database.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - db_name (str): Name of the database file.
    """
    nsd_new_values, nsd_missing_values = generate_nsd_list(db_name)
    nsd_scrape(driver, driver_wait, nsd_new_values)
    nsd_scrape(driver, driver_wait, nsd_missing_values)
    
if __name__ == "__main__":
    # Initialize Selenium WebDriver
    driver, driver_wait = selenium_driver.get_driver()

    # Scrape NSD values
    scrape_nsd_values(driver, driver_wait, settings.db_name)

    # Quit the WebDriver session
    driver.quit()






