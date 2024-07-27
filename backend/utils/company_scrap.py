import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
import sqlite3
import os
import shutil

from utils import system
from utils import selenium_driver
from config import settings

def get_company_ticker(raw_code):
    """
    Extracts company tickers and names from raw HTML code.

    Parameters:
    - raw_code (list): List of raw HTML strings containing company data.

    Returns:
    dict: A dictionary with company names as keys and a nested dictionary with ticker, pregao, and listagem as values.
    """
    # Dictionary to store extracted company information
    company_tickers = {}

    for inner_html in raw_code:
        # Parse the raw HTML source code
        soup = BeautifulSoup(inner_html, 'html.parser')

        # Find all the card elements
        cards = soup.find_all('div', class_='card-body')

        # Loop through each card element and extract the ticker and company name
        for card in cards:
            try:
                # Extract the ticker and company name from the card element
                ticker = system.clean_text(card.find('h5', class_='card-title2').text)
                company_name = system.clean_text(card.find('p', class_='card-title').text)
                pregao = system.clean_text(card.find('p', class_='card-text').text)
                listagem = system.clean_text(card.find('p', class_='card-nome').text)
                if listagem:
                    for abbr, full_name in settings.governance_levels.items():
                        new_listagem = system.clean_text(listagem.replace(abbr, full_name))
                        if new_listagem != listagem:
                            listagem = new_listagem
                            break  # Break out of the loop if a replacement was made

                # Append the ticker and company name to the keyword list
                company_tickers[company_name] = {
                    'ticker': ticker,
                    'pregao': pregao,
                    'listagem': listagem
                }
            except Exception as e:
                system.log_error(e)

    return company_tickers

def get_raw_code(driver, driver_wait, url=settings.companies_url):
    """
    Retrieves raw HTML code from B3 website.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - url (str): URL of the B3 companies page.

    Returns:
    list: A list of raw HTML strings.
    """
    try:
        # Get the total number of companies and pages
        driver.get(url)
        batch = system.choose(f'//*[@id="selectPage"]', driver, driver_wait)

        xpath = '//*[@id="listing_pagination"]/pagination-template/ul'
        text = system.text(xpath, driver_wait)
        pages = re.findall(r'\d+', text)
        pages = list(map(int, pages))
        pages = max(pages) - 1

        raw_code = []
        start_time = time.time()
        for i, page in enumerate(range(0, pages + 1)):
            if i >= 23:
                pass
            xpath = '//*[@id="nav-bloco"]/div'
            system.wait_forever(driver_wait, xpath)
            inner_html = system.raw_text(xpath, driver_wait)
            raw_code.append(inner_html)
            if i != pages:
                system.click(f'//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a', driver_wait)
            extra_info = [f'page {page + 1}']
            system.print_info(i, 0, pages, extra_info, start_time, pages + 1)

    except Exception as e:
        system.log_error(e)
        raw_code = []

    return raw_code

def get_existing_companies(db_name):
    """
    Retrieves a set of existing company names from the database.

    Parameters:
    - db_name (str): Name of the database file.

    Returns:
    set: A set of company names already in the database.
    """
    db_path = os.path.join(settings.db_folder, db_name)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT company_name FROM company_info")
    existing_companies = set(row[0] for row in cursor.fetchall())
    
    conn.close()
    return existing_companies

def save_to_db(data, db_name='company_info.db'):
    """
    Saves company data to the database.

    Parameters:
    - data (list): A list of dictionaries containing company information.
    - db_name (str): Name of the database file.
    """
    try:
        if not data:
            return

        # Backup existing database
        base_name, ext = os.path.splitext(db_name)
        backup_name = f"{base_name} backup{ext}"
        db_path = os.path.join(settings.db_folder, db_name)
        backup_path = os.path.join(settings.db_folder, backup_name)
        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)

        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS company_info (
                            company_name TEXT PRIMARY KEY,
                            ticker TEXT,
                            pregao TEXT,
                            listagem TEXT,
                            cvm_code TEXT,
                            activity TEXT,
                            setor TEXT,
                            subsetor TEXT,
                            segmento TEXT,
                            cnpj TEXT,
                            website TEXT,
                            ticker_codes TEXT,
                            isin_codes TEXT,
                            escriturador TEXT)''')

        # Insert or update records
        for info in data:
            cursor.execute('''INSERT INTO company_info (company_name, ticker, pregao, listagem, cvm_code, activity, setor, subsetor, segmento, cnpj, website, ticker_codes, isin_codes, escriturador)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(company_name) DO UPDATE SET
                            ticker=excluded.ticker,
                            pregao=excluded.pregao,
                            listagem=excluded.listagem,
                            cvm_code=excluded.cvm_code,
                            activity=excluded.activity,
                            setor=excluded.setor,
                            subsetor=excluded.subsetor,
                            segmento=excluded.segmento,
                            cnpj=excluded.cnpj,
                            website=excluded.website,
                            ticker_codes=excluded.ticker_codes,
                            isin_codes=excluded.isin_codes,
                            escriturador=excluded.escriturador''',
                            (info['company_name'], info['ticker'], info.get('pregao', ''), info.get('listagem', ''), info['cvm_code'], info.get('activity', ''),
                             info.get('setor', ''), info.get('subsetor', ''), info.get('segmento', ''), info['cnpj'], info.get('website', ''), 
                             ','.join(info.get('ticker_codes', [])), ','.join(info.get('isin_codes', [])), 
                             info.get('escriturador', '')))

        # Commit and close the connection
        conn.commit()
        conn.close()

        print('Partial save completed...')
    except Exception as e:
        system.log_error(e)

def extract_company_data(detail_soup):
    """
    Extracts detailed company information from the provided BeautifulSoup object.

    Parameters:
    - detail_soup (BeautifulSoup): The BeautifulSoup object containing the company detail page HTML.

    Returns:
    dict: A dictionary containing the extracted company information.
    """
    company_info = detail_soup.find('div', class_='card-body')

    # Extract ticker codes and ISIN codes from the specified XPath
    ticker_codes = []
    isin_codes = []
    accordion_body = detail_soup.find('div', {'id': 'accordionBody2'})
    if (accordion_body):
        rows = accordion_body.find_all('tr')
        for row in rows[1:]:  # Skip the header row
            cols = row.find_all('td')
            if len(cols) > 1:
                ticker_codes.append(system.clean_text(cols[0].text))
                isin_codes.append(system.clean_text(cols[1].text))
    if len(ticker_codes) > 1:
        pass
    # Extract CNPJ
    cnpj_element = company_info.find(text='CNPJ')
    cnpj = re.sub(r'\D', '', cnpj_element.find_next('p', class_='card-linha').text) if cnpj_element else ''
    
    # Extract Activity
    activity_element = company_info.find(text='Atividade Principal')
    activity = activity_element.find_next('p', class_='card-linha').text if activity_element else ''
    
    # Extract Sector Classification
    sector_element = company_info.find(text='Classificação Setorial')
    sector_classification = sector_element.find_next('p', class_='card-linha').text if sector_element else ''
    
    # Extract Website
    website_element = company_info.find(text='Site')
    website = website_element.find_next('a').text if website_element else ''
    
    # Extract Escriturador
    escriturador_element = detail_soup.find(text='Escriturador')
    escriturador = escriturador_element.find_next('span').text.strip() if escriturador_element else ''

    # Splitting sector classification into three levels
    sectors = sector_classification.split('/')
    setor = system.clean_text(sectors[0].strip()) if len(sectors) > 0 else ''
    subsetor = system.clean_text(sectors[1].strip()) if len(sectors) > 1 else ''
    segmento = system.clean_text(sectors[2].strip()) if len(sectors) > 2 else ''

    company_data = {
        "activity": activity,
        "setor": setor,
        "subsetor": subsetor,
        "segmento": segmento, 
        "cnpj": cnpj,
        "website": website,
        "sector_classification": sector_classification,
        "ticker_codes": ticker_codes,
        "isin_codes": isin_codes,
        "escriturador": escriturador,
    }

    return company_data

def get_company_info(driver, driver_wait, company_tickers):
    """
    Retrieves detailed company information for each company in company_tickers.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - company_tickers (dict): A dictionary with company names as keys and ticker information as values.

    Returns:
    dict: A dictionary with company names as keys and detailed company information as values.
    """
    all_company_info = {}
    total_companies = len(company_tickers)
    start_time = time.time()
    all_data = []

    # Get existing companies from the database
    existing_companies = get_existing_companies(settings.db_name)

    # Filter out the companies that are already in the database
    companies_to_process = {name: info for name, info in company_tickers.items() if name not in existing_companies}
    total_companies_to_process = len(companies_to_process)

    for i, (company_name, info) in enumerate(companies_to_process.items()):
        try:
            # Open the search page
            driver.get(settings.company_url)

            # Wait for the search field and enter the company name
            search_field_xpath = '//*[@id="keyword"]'
            search_field = system.wait_forever(driver_wait, search_field_xpath)

            search_field.clear()
            search_field.send_keys(company_name)
            search_field.send_keys(Keys.RETURN)  # Press Enter key

            # Wait for the search results to load
            xpath = '//*[@id="nav-tabContent"]'
            system.wait_forever(driver_wait, xpath)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_='card-body')

            company_found = False
            for card in cards:
                card_ticker = system.clean_text(card.find('h5', class_='card-title2').text)
                if card_ticker == info['ticker']:
                    # Click on the matching card
                    card_xpath = f'//h5[text()="{card_ticker}"]'
                    system.click(card_xpath, driver_wait)
                    
                    # Wait for the company details page to load
                    xpath = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div'
                    system.wait_forever(driver_wait, xpath)

                    # Extract the current URL to get the CVM code
                    match = re.search(r'/main/(\d+)/', driver.current_url)
                    cvm_code = match.group(1) if match else ''
                    info['cvm_code'] = cvm_code

                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')

                    # Extract detailed company information
                    company_data = extract_company_data(detail_soup)

                    info.update(company_data)
                    company_found = True
                    break  # Break out of the loop once the company is found

        except Exception as e:
            system.log_error(f"Error processing company {company_name}: {e}")
            pass

        # Call print_info function to print progress
        all_company_info[company_name] = info
        extra_info = [info['ticker'], info['cvm_code'], company_name]
        system.print_info(i, 0, total_companies_to_process, extra_info, start_time, total_companies_to_process)

        # Collect all data for batch saving
        all_data.append({'company_name': company_name, **info})

        # Save to DB every settings.batch_size iterations or at the end
        if (i + 1) % (settings.batch_size // 5) == 0 or i == total_companies - 1:
            save_to_db(all_data, settings.db_name)
            all_data.clear()

    save_to_db(all_data, settings.db_name)
    print('Final save completed...')
    return all_company_info

if __name__ == "__main__":
    # # Initialize Selenium WebDriver
    # driver, driver_wait = selenium_driver.get_driver()

    # # Retrieve raw HTML code from B3 website
    # raw_code = get_raw_code(driver, driver_wait, settings.companies_url)
    
    # # Extract company tickers and names
    # company_tickers = get_company_ticker(raw_code)

    # # Quit the WebDriver session
    # driver.quit()

    # # Convert company tickers to DataFrame
    # companies_df = pd.DataFrame(company_tickers, columns=['ticker', 'company_name', 'pregao', 'listagem'])
    # companies_df.to_csv('company_data.csv', index=False)

    # # Print completion message
    # print("Data saved to company_data.csv")


    print('this is a module. done!')




