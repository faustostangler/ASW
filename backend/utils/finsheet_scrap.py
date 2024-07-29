import time
import os
from io import StringIO
import shutil
import glob
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
    - db_name (str): Name of the database file.
    
    Returns:
    DataFrame: A DataFrame of NSD values ordered by setor, subsetor, segmento, company, quarter, and version.
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_name)
        
        # Prepare the criteria string for the SQL query
        criteria_str = ', '.join(f"'{c}'" for c in criteria)
        
        # Query to retrieve NSD values based on criteria
        query_nsd = f"""
        SELECT nsd, company, version, quarter 
        FROM nsd 
        WHERE nsd_type IN ({criteria_str})
        """
        df_nsd = pd.read_sql_query(query_nsd, conn)
        
        # Convert necessary columns to string type
        df_nsd['nsd'] = df_nsd['nsd'].astype(str)
        df_nsd['version'] = df_nsd['version'].astype(str)
        
        # Query to retrieve company information
        query_company = "SELECT company_name, cvm_code, setor, subsetor, segmento FROM company_info"
        df_company = pd.read_sql_query(query_company, conn)
        
        # Convert necessary columns to string type
        df_company['cvm_code'] = df_company['cvm_code'].astype(str)
        
        # Merge NSD data with company information
        df_merged = pd.merge(df_nsd, df_company, how='outer', left_on='company', right_on='company_name')
        
        # Fill NaN values with empty strings
        df_merged = df_merged.fillna('')
        
        # Sort and drop duplicates to keep the latest version for each company and quarter
        df_merged = df_merged.sort_values(by=['company', 'quarter', 'version'], ascending=[True, True, True])
        df_merged = df_merged.drop_duplicates(subset=['company', 'quarter'], keep='last')
        
        # Assign a high value for empty categories to sort them last
        last_order = 'ZZZZZZZZZZ'
        df_merged.loc[df_merged['setor'] == '', 'setor'] = last_order
        df_merged.loc[df_merged['subsetor'] == '', 'subsetor'] = last_order
        df_merged.loc[df_merged['segmento'] == '', 'segmento'] = last_order
        
        # Sort by setor, subsetor, segmento, company, quarter, and version
        df_sorted = df_merged.sort_values(by=['setor', 'subsetor', 'segmento', 'company', 'quarter', 'version'])
        
        # Restore empty categories to their original empty string state
        df_sorted.loc[df_sorted['setor'] == last_order, 'setor'] = ''
        df_sorted.loc[df_sorted['subsetor'] == last_order, 'subsetor'] = ''
        df_sorted.loc[df_sorted['segmento'] == last_order, 'segmento'] = ''
        
        # Close the database connection
        conn.close()
        
        # Return the sorted DataFrame
        return df_sorted
    
    except Exception as e:
        # Log any exceptions
        system.log_error(e)
        return []

def save_to_db(df, base_db_name='b3'):
    """
    Saves financial sheet data to separate databases based on the 'setor' value.

    Parameters:
    - df (DataFrame): DataFrame containing financial data.
    - base_db_name (str): Base name of the database file.
    """
    try:
        # If DataFrame is empty, exit the function
        if df.empty:
            return
        
        # Ensure the database folder exists
        if not os.path.exists(settings.db_folder):
            os.makedirs(settings.db_folder)

        # Group DataFrame by 'setor'
        grouped = df.groupby('setor')
        
        for setor, group in grouped:
            # Define database and backup paths
            db_name = f"{base_db_name} {setor}.db"
            db_path = os.path.join(settings.db_folder, db_name)
            base_name, ext = os.path.splitext(db_name)
            backup_name = f"{base_name} backup{ext}"
            backup_path = os.path.join(settings.db_folder, backup_name)
            
            # Backup existing database file if it exists
            if os.path.exists(db_path):
                shutil.copy2(db_path, backup_path)

            # Connect to the sector-specific SQLite database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create 'finsheet' table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS finsheet (
                nsd TEXT, tipo TEXT, setor TEXT, subsetor TEXT, segmento TEXT, company_name TEXT,
                quadro TEXT, quarter TEXT, conta TEXT, descricao TEXT, valor REAL, version TEXT
            )
            ''')

            # Save the group data to the 'finsheet' table
            group.to_sql('finsheet', conn, if_exists='append', index=False)

            # Commit changes and close the connection
            conn.commit()
            conn.close()
    
        print('Partial save completed...')

    except Exception as e:
        # Log any exceptions
        system.log_error(e)

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
    DataFrame: A DataFrame containing the scraped data.
    """
    try:
        # XPaths for selecting group and frame combo boxes
        xpath_grupo = '//*[@id="cmbGrupo"]'
        xpath_quadro = '//*[@id="cmbQuadro"]'

        # Wait for the group combo box and select the specified value
        element_grupo = system.wait_forever(driver_wait, xpath_grupo)
        select_grupo = Select(driver.find_element(By.XPATH, xpath_grupo))
        select_grupo.select_by_visible_text(cmbGrupo)
        
        # Wait for the frame combo box and select the specified value
        element_quadro = system.wait_forever(driver_wait, xpath_quadro)
        select_quadro = Select(driver.find_element(By.XPATH, xpath_quadro))
        select_quadro.select_by_visible_text(cmbQuadro)

        # Wait for the frame and switch to it
        xpath = '//*[@id="iFrameFormulariosFilho"]'
        frame = system.wait_forever(driver_wait, xpath)
        frame = driver.find_elements(By.XPATH, xpath)
        driver.switch_to.frame(frame[0])

        # Check if values are in thousands
        thousand = 1
        xpath_thousand_check = '//*[@id="UltimaTabela"]/table/tbody[1]/tr[1]'
        element = system.wait_forever(driver_wait, xpath_thousand_check)
        text = element.text
        if 'Mil' in text:
            thousand = 1000

        # XPaths for scraping share data
        acoes_on_xpath = '//*[@id="QtdAordCapiItgz_1"]'
        acoes_pn_xpath = '//*[@id="QtdAprfCapiItgz_1"]'
        acoes_on_tesouraria_xpath = '//*[@id="QtdAordTeso_1"]'
        acoes_pn_tesouraria_xpath = '//*[@id="QtdAprfTeso_1"]'

        # Scrape the share data
        acoes_on = driver.find_element(By.XPATH, acoes_on_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_pn = driver.find_element(By.XPATH, acoes_pn_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_on_tesouraria = driver.find_element(By.XPATH, acoes_on_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')
        acoes_pn_tesouraria = driver.find_element(By.XPATH, acoes_pn_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')

        # Prepare data for DataFrame
        data = {
            'conta': ['00.01.01', '00.01.02', '00.02.01', '00.02.02'],
            'descricao': ['Ações Ordinárias ON', 'Ações Preferenciais PN', 'Ações em Tesouraria Ordinárias ON', 'Ações em Tesouraria Preferenciais PN'],
            'valor': [float(acoes_on) * thousand, float(acoes_pn) * thousand, float(acoes_on_tesouraria) * thousand, float(acoes_pn_tesouraria) * thousand]
        }

        # Create DataFrame from scraped data
        df = pd.DataFrame(data)

        # Switch back to the parent frame
        driver.switch_to.parent_frame()
        
        # Return the DataFrame
        return df
    
    except Exception as e:
        # Log any exceptions
        system.log_error(e)
        return None

def scrape_financial_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter):
    """
    Scrapes financial data from the specified page.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - cmbGrupo: The group combo box value.
    - cmbQuadro: The frame combo box value.
    - quarter: The quarter date.

    Returns:
    DataFrame: A DataFrame containing the scraped data.
    """
    try:
        # XPaths for selecting group and frame combo boxes
        xpath_grupo = '//*[@id="cmbGrupo"]'
        xpath_quadro = '//*[@id="cmbQuadro"]'

        # Wait for the group combo box and select the specified value
        element_grupo = system.wait_forever(driver_wait, xpath_grupo)
        select_grupo = Select(driver.find_element(By.XPATH, xpath_grupo))
        select_grupo.select_by_visible_text(cmbGrupo)
        
        # Wait for the frame combo box and select the specified value
        element_quadro = system.wait_forever(driver_wait, xpath_quadro)
        select_quadro = Select(driver.find_element(By.XPATH, xpath_quadro))
        select_quadro.select_by_visible_text(cmbQuadro)

        # Wait for the frame and switch to it
        xpath = '//*[@id="iFrameFormulariosFilho"]'
        frame = system.wait_forever(driver_wait, xpath)
        frame = driver.find_elements(By.XPATH, xpath)
        driver.switch_to.frame(frame[0])

        # Check if the table is empty
        xpath_table = '//*[@id="ctl00_cphPopUp_tbDados"]'
        table = driver.find_element(By.XPATH, xpath_table)
        if not table.find_elements(By.TAG_NAME, "tr"):
            driver.switch_to.parent_frame()
            return None

        # Determine if the values are in thousands
        xpath = '//*[@id="TituloTabelaSemBorda"]'
        thousand_text = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath))).text
        thousand = 1000 if "Mil" in thousand_text else 1

        # Read HTML content and parse it into DataFrames
        html_content = driver.page_source
        df1 = pd.read_html(StringIO(html_content), header=0)[0]
        df2 = pd.read_html(StringIO(html_content), header=0, thousands='.')[0].fillna(0)

        # Rename columns and merge DataFrames
        columns = ['conta', 'descricao', 'valor']
        df1 = df1.iloc[:,0:3]
        df2 = df2.iloc[:,0:3]
        df1.columns = columns
        df2.columns = columns
        df = pd.concat([df1.iloc[:, :2], df2.iloc[:, 2:3]], axis=1)

        # Convert values to numeric and apply the thousand multiplier
        col = df.iloc[:, 2].astype(str)
        col = col.str.replace('.', '', regex=False)
        col = col.str.replace(',', '.', regex=False)
        col = pd.to_numeric(col, errors='coerce')
        col = col * thousand
        df.iloc[:, 2] = col

        # Switch back to the parent frame
        driver.switch_to.parent_frame()
        
        # Return the DataFrame
        return df
    
    except Exception as e:
        return None

def load_existing_data(db_folder=settings.db_folder, db_name=settings.db_name):
    """
    Loads all necessary data from multiple database files and returns it for in-memory checks.

    Parameters:
    - db_folder (str): The folder where databases are stored.
    - db_name (str): The name of the database file to derive the prefix from.

    Returns:
    DataFrame: DataFrame containing all necessary data for comparison.
    """
    base_name, ext = os.path.splitext(db_name)
    base_db_prefix = f"{base_name} "

    all_dfs = []
    db_files = glob.glob(f"{db_folder}/{base_db_prefix}*.db")
    
    valid_db_files = [db_file for db_file in db_files if 'backup' not in db_file]

    start_time = time.time()
    for i, db_file in enumerate(valid_db_files):
        conn = sqlite3.connect(db_file)
        query = "SELECT nsd, company_name, tipo, quadro, quarter, version FROM finsheet"
        df = pd.read_sql_query(query, conn)
        conn.close()
        all_dfs.append(df)
        
        extra_info = [db_file]
    
    if all_dfs:
        all_dfs = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
        return all_dfs
    else:
        return pd.DataFrame(columns=['nsd', 'company_name', 'tipo', 'quadro', 'quarter', 'version'])

def filter_nsd_data(df_nsd):
    """
    Filters out rows from df_nsd that already have up-to-date versions in the existing data.

    Parameters:
    - df_nsd (DataFrame): DataFrame containing new nsd data.
    - existing_data (DataFrame): DataFrame containing existing nsd data.

    Returns:
    DataFrame: Filtered DataFrame with only new or updated rows.
    """
    # Load existing data for comparison
    existing_data = load_existing_data(settings.db_folder) 

    # Ensure 'quarter' columns are in datetime format
    df_nsd.loc[:, 'quarter'] = pd.to_datetime(df_nsd['quarter'], dayfirst=False, errors='coerce').dt.strftime('%Y-%m-%d')
    existing_data.loc[:, 'quarter'] = pd.to_datetime(existing_data['quarter'], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Ensure 'version' columns are strings
    df_nsd.loc[:, 'version'] = df_nsd['version'].astype(str)
    existing_data.loc[:, 'version'] = existing_data['version'].astype(str)

    # Aggregate existing data to get the maximum version for each company and quarter
    existing_data_agg = existing_data.groupby(['company_name', 'quarter'])['version'].max().reset_index()
    existing_data_agg.rename(columns={'version': 'max_version_existing'}, inplace=True)

    # Merge new NSD data with the aggregated existing data
    merged = df_nsd.merge(existing_data_agg, on=['company_name', 'quarter'], how='left')

    # Filter rows where the new version is greater than the existing version or if there is no existing version
    df_nsd_missing = merged[
        (merged['version'] > merged['max_version_existing']) |
        (merged['max_version_existing'].isna())
    ].drop(columns=['max_version_existing'])

    return df_nsd_missing

def finsheet_scrape(driver, driver_wait, df_nsd):
    """
    Scrapes financial and capital data for the specified NSD values.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - df_nsd (DataFrame): DataFrame containing NSD values to scrape.

    Returns:
    DataFrame: DataFrame containing all scraped data.
    """
    # Filter NSD data to get only the new or updated rows
    print('loading database...')
    df_nsd_missing = filter_nsd_data(df_nsd)

    start_time_nsd = time.time()
    size_nsd = len(df_nsd_missing)
    counter = 0

    all_data = []

    for index, row in df_nsd_missing.iterrows():
        # Prepare necessary information for scraping
        quarter = row['quarter']
        extra_info_nsd = [row['nsd'], row['company_name'], quarter]
        system.print_info(counter, 0, size_nsd, extra_info_nsd, start_time_nsd, size_nsd)
        counter += 1

        # Construct URL and navigate to it
        url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={row['nsd']}&CodigoTipoInstituicao=1"
        driver.get(url)

        company_quarter_data = []

        # Scrape financial data
        for i, (cmbGrupo, cmbQuadro) in enumerate(settings.findata):
            df = scrape_financial_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter)
            if df is not None:
                company_quarter_data.append((row['nsd'], row['company_name'], quarter, row['setor'], row['subsetor'], row['segmento'], row['version'], cmbGrupo, cmbQuadro, df))

        # Scrape capital data
        for i, (cmbGrupo, cmbQuadro) in enumerate(settings.fincapital):
            df = scrape_capital_data(driver, driver_wait, cmbGrupo, cmbQuadro, quarter)
            if df is not None:
                company_quarter_data.append((row['nsd'], row['company_name'], quarter, row['setor'], row['subsetor'], row['segmento'], row['version'], cmbGrupo, cmbQuadro, df))

        # Prepare data for saving
        for nsd, company_name, quarter, setor, subsetor, segmento, version, cmbGrupo, cmbQuadro, df in company_quarter_data:
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

        # Save data in batches
        if (counter + 1) % int(settings.batch_size/10) == 0 or counter == size_nsd - 1:
            finsheet = pd.concat(all_data, ignore_index=True)
            columns = ['nsd', 'tipo', 'setor', 'subsetor', 'segmento', 'company_name', 'quadro', 'quarter', 'conta', 'descricao', 'valor', 'version']
            finsheet = finsheet[columns]
            finsheet = finsheet.sort_values(by=['conta', 'descricao'])
            save_to_db(finsheet)
            all_data.clear()

    print('done')
    return finsheet

def main_multiple(driver, driver_wait, batch_size=settings.big_batch_size, batch=1):
    """
    Main function to scrape NSD data in batches.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - batch_size (int): Size of each batch to process.
    - batch (int): Batch number to start processing from.
    """
    # Retrieve NSD data based on criteria
    df_nsd = get_nsd_data(settings.finsheet_types)
    print('loading database...')
    df_nsd_missing = filter_nsd_data(df_nsd)
    num_batches = settings.num_batches
    batch_size = int(len(df_nsd_missing)/4)
    for i in range(num_batches):
        if i == batch:
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            df_nsd_batch = df_nsd_missing[start_idx:end_idx]
            if not df_nsd_batch.empty:
                finsheet_scrape(driver, driver_wait, df_nsd_batch)

def main(driver, driver_wait, batch_size=settings.big_batch_size, batch=1):
    """
    Main function to scrape NSD data in batches.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - batch_size (int): Size of each batch to process.
    - batch (int): Batch number to start processing from.
    """
    # Retrieve NSD data based on criteria
    df_nsd = get_nsd_data(settings.finsheet_types)
    if not df_nsd.empty:
        finsheet_scrape(driver, driver_wait, df_nsd)
if __name__ == "__main__":
    print('this is a module. done!')
