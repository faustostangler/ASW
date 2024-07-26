# finsheet.py

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

        df1.rename(columns={df1.columns[2]: quarter}, inplace=True)
        df2.rename(columns={df2.columns[2]: quarter}, inplace=True)
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

def main(driver, driver_wait, finsheet=None):
    
    df_nsd = get_nsd_data(settings.finsheet_types)

    all_data = []
    for index, row in df_nsd.iterrows():
        quarter = pd.to_datetime(row['quarter'], dayfirst=False, errors='coerce').strftime('%Y-%m-%d')

        url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={row['nsd']}&CodigoTipoInstituicao=1"
        driver.get(url)

        for cmbGrupo, cmbQuadro in settings.findata: 
            df = scrape_financial_data(driver, driver_wait,  cmbGrupo, cmbQuadro, quarter)
            if df is not None:
                all_data.append((row['nsd'], row['company_name'], quarter, row['setor'], row['subsetor'], row['segmento'], row['version'], cmbGrupo, cmbQuadro, df))

        dfi = []
        dfc = []

        # Process each tuple in the all_data list
        for nsd, company_name, quarter, setor, subsetor, segmento, version, cmbGrupo, cmbQuadro, df in all_data:
            # Insert columns for nsd, cmbGrupo, and cmbQuadro
            df.insert(0, 'nsd', nsd)
            df.insert(0, 'company_name', company_name)
            df.insert(0, 'quarter', quarter)
            df.insert(0, 'version', version)
            df.insert(0, 'segmento', segmento)
            df.insert(0, 'subsetor', subsetor)
            df.insert(0, 'setor', setor)
            df.insert(0, 'tipo', cmbQuadro)
            df.insert(0, 'quadro', cmbGrupo)

            # Append the DataFrame to the appropriate list based on cmbGrupo
            if 'Individuais' in cmbGrupo:
                dfi.append(df)
            elif 'Consolidadas' in cmbGrupo:
                dfc.append(df)


        columns = ['nsd', 'tipo', 'setor', 'subsetor', 'segmento', 'company_name', 
                            'quadro', 'conta', 'descrição', 'quarter', 'version']
        # Concatenate all DataFrames in each list into single DataFrames
        if dfi:
            df_individual = pd.concat(dfi, ignore_index=True).sort_values(by='Conta')
        else:
            df_individual = pd.DataFrame(columns=columns)

        # Handle df_consolidada
        if dfc:
            df_consolidada = pd.concat(dfc, ignore_index=True).sort_values(by='Conta')
        else:
            df_consolidada = pd.DataFrame(columns=columns)
    print('done')

    return df_individual[columns], df_consolidada[columns]

if __name__ == "__main__":
    finsheet = main()
    print('done')
