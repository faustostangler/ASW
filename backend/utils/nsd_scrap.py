import time

import sqlite3
import pandas as pd

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
from datetime import datetime, timedelta

import utils.system as system
from config import settings

def parse_data(driver, wait, i):
    data = {}
    try:
        data['nsd'] = i

        company_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="lblNomeCompanhia"]')))
        data['company'] = system.clean_text(company_element.text)

        dri_element = driver.find_element(By.XPATH, '//*[@id="lblNomeDRI"]')
        data['dri'] = system.clean_text(dri_element.text.split('-')[0].strip())

        nsd_type_element = driver.find_element(By.XPATH, '//*[@id="lblDescricaoCategoria"]')
        nsd_type_version = nsd_type_element.text
        data['nsd_type'] = system.clean_text(nsd_type_version.split('-')[0].strip())
        data['version'] = int(re.search(r'V(\d+)', nsd_type_version).group(1))

        auditor_element = driver.find_element(By.XPATH, '//*[@id="lblAuditor"]')
        data['auditor'] = system.clean_text(auditor_element.text.split('-')[0].strip())

        auditor_rt_element = driver.find_element(By.XPATH, '//*[@id="lblResponsavelTecnico"]')
        data['auditor_rt'] = system.clean_text(auditor_rt_element.text)

        protocolo_element = driver.find_element(By.XPATH, '//*[@id="lblProtocolo"]')
        data['protocolo'] = protocolo_element.text.replace('-', '').strip()

        date_element = driver.find_element(By.XPATH, '//*[@id="lblDataDocumento"]')
        date_str = date_element.text
        data['date'] = pd.to_datetime(date_str, dayfirst=True, errors='coerce')

        sent_date_element = driver.find_element(By.XPATH, '//*[@id="lblDataEnvio"]')
        sent_date_str = sent_date_element.text
        data['sent_date'] = pd.to_datetime(sent_date_str, dayfirst=True, errors='coerce')

        reason_element = driver.find_element(By.XPATH, '//*[@id="lblMotivoCancelamentoReapresentacao"]')
        data['reason'] = system.clean_text(reason_element.text)
    except Exception as e:
        # system_utils.log_error(e)
        pass

    return data if 'sent_date' in data else None

def save_to_db(data, db_name='b3.db'):
    try:
        if not data:
            return
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS nsd
                        (nsd INTEGER PRIMARY KEY, company TEXT, dri TEXT, nsd_type TEXT, version INTEGER, auditor TEXT,
                        auditor_rt TEXT, protocolo TEXT, date TEXT, sent_date TEXT, reason TEXT)''')

        for item in data:
            date_str = item['date'].isoformat() if item['date'] else None
            sent_date_str = item['sent_date'].isoformat() if item['sent_date'] else None

            cursor.execute('''INSERT INTO nsd 
                            (nsd, company, dri, nsd_type, version, auditor, auditor_rt, protocolo, date, sent_date, reason) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(nsd) DO UPDATE SET
                            company = excluded.company,
                            dri = excluded.dri,
                            nsd_type = excluded.nsd_type,
                            version = excluded.version,
                            auditor = excluded.auditor,
                            auditor_rt = excluded.auditor_rt,
                            protocolo = excluded.protocolo,
                            date = excluded.date,
                            sent_date = excluded.sent_date,
                            reason = excluded.reason
                            WHERE excluded.sent_date > nsd.sent_date''', 
                            (item['nsd'], item['company'], item['dri'], item['nsd_type'], item['version'], 
                            item['auditor'], item['auditor_rt'], item['protocolo'], 
                            date_str, sent_date_str, item['reason']))

        conn.commit()
        conn.close()

        print('partial save...')
    except Exception as e:
        system.log_error(e)

def generate_nsd_list(db_name):
    """
    Generates a list of new nsd numbers to scrape and finds missing nsd values.

    Parameters:
    - db_name (str): The name of the SQLite database file.

    Returns:
    tuple: A tuple with two elements:
           - A list of new nsd numbers generated based on date difference.
           - A list of missing nsd values from the database.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Fetch the nsd data
    cursor.execute('SELECT nsd, MIN(sent_date), MAX(sent_date) FROM nsd')
    result = cursor.fetchone()
    
    # Retrieve all existing nsd values
    cursor.execute("SELECT nsd FROM nsd ORDER BY nsd;")
    existing_nsds = cursor.fetchall()
    
    # Close the database connection
    conn.close()
    
    # Convert list of tuples to a list of integers
    existing_nsds = [nsd[0] for nsd in existing_nsds]
    
    if not existing_nsds:
        return ([], [])  # Return tuple with empty lists if no nsd values are present
    
    # Determine the maximum nsd value
    max_nsd = max(existing_nsds)
    
    # Generate new nsd list based on date difference
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
    
    # Create a set of all possible nsd values in the range 1 to max_nsd
    full_set = set(range(1, max_nsd + 1))
    existing_set = set(existing_nsds)
    
    # Find missing nsd values
    nsd_missing_values = sorted(full_set - existing_set)
    
    return nsd_new_values, nsd_missing_values

def nsd_scrape(driver, wait, nsd_list):
    try:
        db_name = 'b3.db'
        all_data = []
        size = len(nsd_list)
        start_time = time.time()

        for i, nsd in enumerate(nsd_list):
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            driver.get(url)

            data = parse_data(driver, wait, nsd)

            if data:
                all_data.append(data)
                data_tuple = (data['sent_date'], data['nsd_type'], data['date'], data['company'])
            else:
                data_tuple = ('', '', '', '')

            system.print_info(i, nsd_list[0], nsd_list[-1], data_tuple, start_time, size)

            if (i + 1) % settings.batch_size == 0 or i == size - 1:
                save_to_db(all_data, db_name)
                all_data.clear()

        print("Scraping completed.")
    except Exception as e:
        system.log_error(e)

if __name__ == "__main__":
    from utils import selenium_driver as drv
    driver, wait = drv.get_driver()
    nsd_scrape(driver, wait, nsd_list)
    driver.quit()
