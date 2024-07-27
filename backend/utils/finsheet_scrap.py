import time
import os
import shutil
from io import StringIO
import pandas as pd
import sqlite3
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC

from utils import system
from config import settings

def get_nsd_data(criteria=settings.finsheet_types, db_name=f'{settings.db_folder}/{settings.db_name}'):
    """
    Retrieve NSD values based on criteria and perform an outer merge with company_info table.
    
    Parameters:
    - criteria (list): List of nsd_type criteria to filter NSD values.

    Returns:
    list: A list of NSD values ordered by setor, subsetor, segmento, company, quarter, and version.
    """
    try:
        # Connect to the database
        
        conn = sqlite3.connect(db_name)
        
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
        return df_sorted
    
    except Exception as e:
        system.log_error(e)
        return []

import os
import shutil
import pandas as pd
import sqlite3

def save_to_db(df, base_db_name='b3'):
    """
    Saves financial sheet data to separate databases based on the 'setor' value.

    Parameters:
    - df (DataFrame): DataFrame containing financial data.
    - base_db_name (str): Base name of the database file.
    """
    try:
        if df.empty:
            return
        
        # Ensure the directory exists
        if not os.path.exists(settings.db_folder):
            os.makedirs(settings.db_folder)

        # Group by 'setor' and save each group into a separate database
        grouped = df.groupby('setor')
        for setor, group in grouped:
            # Generate the database name based on the 'setor' value
            db_name = f"{base_db_name} {setor}.db"
            db_path = os.path.join(settings.db_folder, db_name)
            
            # Backup existing database
            base_name, ext = os.path.splitext(db_name)
            backup_name = f"{base_name} backup{ext}"
            backup_path = os.path.join(settings.db_folder, backup_name)
            if os.path.exists(db_path):
                shutil.copy2(db_path, backup_path)

            # Connect to the database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create finsheet table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS finsheet (
                nsd TEXT, tipo TEXT, setor TEXT, subsetor TEXT, segmento TEXT, company_name TEXT,
                quadro TEXT, quarter TEXT, conta TEXT, descricao TEXT, valor REAL, version TEXT
            )
            ''')

            # Insert or update records
            group.to_sql('finsheet', conn, if_exists='append', index=False)

            # Commit and close the connection
            conn.commit()
            conn.close()

    except Exception as e:
        system.log_error(e)

# Example usage:
# save_to_db(finsheet)


def scrape_financial_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter):
    
    try:
        xpath_grupo = '//*[@id="cmbGrupo"]'
        xpath_quadro = '//*[@id="cmbQuadro"]'

        # Select the correct options for cmbGrupo and cmbQuadro
        element_grupo = system.wait_forever(driver_wait, xpath_grupo)
        select_grupo = Select(driver.find_element(By.XPATH, xpath_grupo))
        select_grupo.select_by_visible_text(cmbGrupo)
        
        element_quadro = system.wait_forever(driver_wait, xpath_quadro)
        select_quadro = Select(driver.find_element(By.XPATH, xpath_quadro))
        select_quadro.select_by_visible_text(cmbQuadro)

        # selenium enter frame
        xpath = '//*[@id="iFrameFormulariosFilho"]'
        frame = system.wait_forever(driver_wait, xpath)
        frame = driver.find_elements(By.XPATH, xpath)
        driver.switch_to.frame(frame[0])

        # read and clean quadro
        xpath = '//*[@id="ctl00_cphPopUp_tbDados"]'
        thousand = system.wait_forever(driver_wait, xpath)

        xpath = '//*[@id="TituloTabelaSemBorda"]'
        thousand = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath))).text
        thousand = 1000 if "Mil" in thousand else 1

        html_content = driver.page_source
        df1 = pd.read_html(StringIO(html_content), header=0)[0]
        df2 = pd.read_html(StringIO(html_content), header=0, thousands='.')[0].fillna(0)

        columns = ['conta', 'descricao', 'valor']
        df1 = df1.iloc[:,0:3]
        df2 = df2.iloc[:,0:3]
        df1.columns = columns
        df2.columns = columns
        df = pd.concat([df1.iloc[:, :2], df2.iloc[:, 2:3]], axis=1)

        col = df.iloc[:, 2].astype(str)
        col = col.str.replace('.', '', regex=False)
        col = col.str.replace(',', '.', regex=False)
        col = pd.to_numeric(col, errors='coerce')
        col = col * thousand
        df.iloc[:, 2] = col

        # selenium exit frame
        driver.switch_to.parent_frame()
        
        return df
    
    except Exception as e:
        # system.log_error(e)
        return None

def check_existing_data(setor, company_name, quarter, version, db_folder, base_db_name='b3.db'):
    """
    Check if the company and quarter already exist in the database and if the version is newer.

    Parameters:
    - setor (str): The sector of the company.
    - company_name (str): The name of the company.
    - quarter (str): The quarter date in 'YYYY-MM-DD' format.
    - version (str): The version of the data.
    - db_folder (str): The folder where databases are stored.
    - base_db_name (str): Base name of the database file.

    Returns:
    - bool: True if the data needs to be scraped, False otherwise.
    """
    base_name, ext = os.path.splitext(base_db_name)
    db_name = f"{base_name} {setor}{ext}"
    db_path = os.path.join(db_folder, db_name)

    if not os.path.exists(db_path):
        return True  # Database doesn't exist, so we need to scrape the data.

    try:
        conn = sqlite3.connect(db_path)
        query = """
        SELECT version FROM finsheet
        WHERE company_name = ? AND quarter = ?
        ORDER BY version DESC LIMIT 1
        """
        cursor = conn.cursor()
        cursor.execute(query, (company_name, quarter))
        result = cursor.fetchone()
        conn.close()

        if result is None:
            return True  # No existing data for this company and quarter.
        existing_version = result[0]
        return version > existing_version  # Only scrape if the new version is greater.

    except Exception as e:
        system.log_error(e)
        return True  # If there's an error, assume we need to scrape the data.

def scrape_capital_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter):
    """
    Scrapes capital data from the specified page.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - cmbGrupo: The group combo box value.
    - cmbQuadro: The frame combo box value.
    - quarter: The quarter date.

    Returns:
    - DataFrame: A DataFrame containing the scraped data.
    """
    try:
        xpath_grupo = '//*[@id="cmbGrupo"]'
        xpath_quadro = '//*[@id="cmbQuadro"]'

        # Select the correct options for cmbGrupo and cmbQuadro
        element_grupo = system.wait_forever(driver_wait, xpath_grupo)
        select_grupo = Select(driver.find_element(By.XPATH, xpath_grupo))
        select_grupo.select_by_visible_text(cmbGrupo)
        
        element_quadro = system.wait_forever(driver_wait, xpath_quadro)
        select_quadro = Select(driver.find_element(By.XPATH, xpath_quadro))
        select_quadro.select_by_visible_text(cmbQuadro)

        # selenium enter frame
        xpath = '//*[@id="iFrameFormulariosFilho"]'
        frame = system.wait_forever(driver_wait, xpath)
        frame = driver.find_elements(By.XPATH, xpath)
        driver.switch_to.frame(frame[0])

        # Determine if the values are in thousands
        thousand = 1
        xpath_thousand_check = '//*[@id="UltimaTabela"]/table/tbody[1]/tr[1]'
        element = system.wait_forever(driver_wait, xpath_thousand_check)
        text = element.text
        if 'Mil' in text:
            thousand = 1000

        # Extract the required values
        acoes_on_xpath = '//*[@id="QtdAordCapiItgz_1"]'
        acoes_pn_xpath = '//*[@id="QtdAprfCapiItgz_1"]'
        acoes_on_tesouraria_xpath = '//*[@id="QtdAordTeso_1"]'
        acoes_pn_tesouraria_xpath = '//*[@id="QtdAprfTeso_1"]'

        acoes_on = driver.find_element(By.XPATH, acoes_on_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_pn = driver.find_element(By.XPATH, acoes_pn_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_on_tesouraria = driver.find_element(By.XPATH, acoes_on_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_pn_tesouraria = driver.find_element(By.XPATH, acoes_pn_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')

        data = {
            'conta': ['00.01.01', '00.01.02', '00.02.01', '00.02.02'],
            'descricao': ['Ações Ordinárias ON', 'Ações Preferenciais PN', 'Ações em Tesouraria Ordinárias ON', 'Ações em Tesouraria Preferenciais PN'],
            'valor': [float(acoes_on) * thousand, float(acoes_pn) * thousand, float(acoes_on_tesouraria) * thousand, float(acoes_pn_tesouraria) * thousand]
        }

        df = pd.DataFrame(data)

        # selenium exit frame
        driver.switch_to.parent_frame()
        
        return df
    
    except Exception as e:
        system.log_error(e)
        return None

def main(driver, driver_wait):
    df_nsd = get_nsd_data(settings.finsheet_types).reset_index(drop=True)

    start_time_nsd = time.time()
    size_nsd = len(df_nsd)
    counter = 0

    all_data = []

    for index, row in df_nsd.iterrows():
        quarter = pd.to_datetime(row['quarter'], dayfirst=False, errors='coerce').strftime('%Y-%m-%d')

        if not check_existing_data(row['setor'], row['company_name'], quarter, row['version'], settings.db_folder, base_db_name='b3.db'):
            continue  # Skip scraping if data already exists and is up-to-date.

        url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={row['nsd']}&CodigoTipoInstituicao=1"
        driver.get(url)

        company_quarter_data = []  # Clear company_quarter_data for each company

        for i, (cmbGrupo, cmbQuadro) in enumerate(settings.fincapital):
            df = scrape_capital_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter)
            if df is not None:
                company_quarter_data.append((row['nsd'], row['company_name'], quarter, row['setor'], row['subsetor'], row['segmento'], row['version'], cmbGrupo, cmbQuadro, df))

        for i, (cmbGrupo, cmbQuadro) in enumerate(settings.findata):
            df = scrape_financial_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter)
            if df is not None:
                company_quarter_data.append((row['nsd'], row['company_name'], quarter, row['setor'], row['subsetor'], row['segmento'], row['version'], cmbGrupo, cmbQuadro, df))

        # Process each tuple in the company_quarter_data list
        for nsd, company_name, quarter, setor, subsetor, segmento, version, cmbGrupo, cmbQuadro, df in company_quarter_data:
            # Insert columns for nsd, cmbGrupo, and cmbQuadro
            df.insert(0, 'nsd', nsd)
            df.insert(0, 'company_name', company_name)
            df.insert(0, 'quarter', quarter)
            df.insert(0, 'version', version)
            df.insert(0, 'segmento', segmento)
            df.insert(0, 'subsetor', subsetor)
            df.insert(0, 'setor', setor)
            df.insert(0, 'tipo', cmbGrupo)
            df.insert(0, 'quadro', cmbQuadro)

            all_data.append(df)

        extra_info_nsd = [row['nsd'], row['company_name'], quarter]
        system.print_info(counter, 0, size_nsd, extra_info_nsd, start_time_nsd, size_nsd)
        counter += 1

        # Save to DB every settings.batch_size iterations or at the end
        if (counter + 1) % settings.batch_size == 0 or counter == size_nsd - 1:
            finsheet = pd.concat(all_data, ignore_index=True)
            columns = ['nsd', 'tipo', 'setor', 'subsetor', 'segmento', 'company_name', 'quadro', 'quarter', 'conta', 'descricao', 'valor', 'version']
            finsheet = finsheet[columns]
            finsheet = finsheet.sort_values(by=['conta', 'descricao'])

            save_to_db(finsheet)
            all_data.clear()

    print('done')
    return True

if __name__ == "__main__":
    print('this is a module. done!')
    # columns = ['nsd', 'tipo', 'setor', 'subsetor', 'segmento', 'company_name', 'quadro', 'conta', 'descrição', 'quarter', 'version']
