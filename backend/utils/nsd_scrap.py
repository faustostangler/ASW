import time

import sqlite3
import pandas as pd
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
import string
from datetime import datetime, timedelta
import unidecode

from utils import system_utils
from config import settings as setup

def clean_text(text):
    try:
        text = unidecode.unidecode(text).translate(str.maketrans('', '', string.punctuation)).upper().strip()
        text = re.sub(r'\s+', ' ', text)
    except Exception as e:
        system_utils.log_error(e)
    return text

def parse_data(driver, wait, i):
    data = {}
    try:
        data['nsd'] = i

        company_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="lblNomeCompanhia"]')))
        data['company'] = clean_text(company_element.text)

        dri_element = driver.find_element(By.XPATH, '//*[@id="lblNomeDRI"]')
        data['dri'] = clean_text(dri_element.text.split('-')[0].strip())

        nsd_type_element = driver.find_element(By.XPATH, '//*[@id="lblDescricaoCategoria"]')
        nsd_type_version = nsd_type_element.text
        data['nsd_type'] = clean_text(nsd_type_version.split('-')[0].strip())
        data['version'] = int(re.search(r'V(\d+)', nsd_type_version).group(1))

        auditor_element = driver.find_element(By.XPATH, '//*[@id="lblAuditor"]')
        data['auditor'] = clean_text(auditor_element.text.split('-')[0].strip())

        auditor_rt_element = driver.find_element(By.XPATH, '//*[@id="lblResponsavelTecnico"]')
        data['auditor_rt'] = clean_text(auditor_rt_element.text)

        protocolo_element = driver.find_element(By.XPATH, '//*[@id="lblProtocolo"]')
        data['protocolo'] = protocolo_element.text.replace('-', '').strip()

        date_element = driver.find_element(By.XPATH, '//*[@id="lblDataDocumento"]')
        date_str = date_element.text
        data['date'] = pd.to_datetime(date_str, dayfirst=True, errors='coerce')

        sent_date_element = driver.find_element(By.XPATH, '//*[@id="lblDataEnvio"]')
        sent_date_str = sent_date_element.text
        data['sent_date'] = pd.to_datetime(sent_date_str, dayfirst=True, errors='coerce')

        reason_element = driver.find_element(By.XPATH, '//*[@id="lblMotivoCancelamentoReapresentacao"]')
        data['reason'] = clean_text(reason_element.text)
    except Exception as e:
        # system_utils.log_error(e)
        return None

    return data if 'company' in data else None

def save_to_db(data, db_name='nsd.db'):
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
        system_utils.log_error(e)

def calculate_end(db_name):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute('SELECT nsd, MIN(sent_date), MAX(sent_date) FROM nsd')
        result = cursor.fetchone()
        conn.close()

        if result and result[0]:
            last_nsd = result[0]
            first_date = pd.to_datetime(result[1])
            last_date = pd.to_datetime(result[2])

            days_diff_total = (last_date - first_date).days
            items_per_day = last_nsd / days_diff_total if days_diff_total != 0 else 0

            current_date = pd.to_datetime(datetime.now())
            days_to_current = (current_date - last_date).days
            remaining_items = int(items_per_day * days_to_current)

            return last_nsd, last_nsd + remaining_items
    except Exception as e:
        system_utils.log_error(e)

    return 0, 0

def nsd_scrape(driver, wait):
    try:
        db_name = 'nsd.db'
        start, end = calculate_end(db_name)
        if start >= 1:
            start += 1

        all_data = []

        start_time = time.time()
        for nsd in range(start, end + 1, setup.batch_size):
            batch_end = min(nsd + setup.batch_size - 1, end)

            for i in range(nsd, batch_end + 1):
                url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={i}&CodigoTipoInstituicao=1"
                driver.get(url)

                data = parse_data(driver, wait, i)

                if data:
                    all_data.append(data)
                    data = (data['sent_date'], data['nsd_type'], data['date'], data['company'])
                else:
                    data = ''

                system_utils.print_info(i, start, end, start_time, data)

            save_to_db(all_data, db_name)
            all_data.clear()

        print("Scraping completed.")
    except Exception as e:
        system_utils.log_error(e)

if __name__ == "__main__":
    from utils import selenium_driver as drv
    driver, wait = drv.get_driver()
    nsd_scrape(driver, wait)
    driver.quit()
