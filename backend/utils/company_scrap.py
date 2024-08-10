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
    company_tickers = {}

    for inner_html in raw_code:
        soup = BeautifulSoup(inner_html, 'html.parser')
        card_body_class = 'card-body'
        cards = soup.find_all('div', class_=card_body_class)

        for card in cards:
            try:
                ticker_class = 'card-title2'
                company_name_class = 'card-title'
                pregao_class = 'card-text'
                listagem_class = 'card-nome'

                ticker = system.clean_text(card.find('h5', class_=ticker_class).text)
                company_name = system.clean_text(card.find('p', class_=company_name_class).text)
                pregao = system.clean_text(card.find('p', class_=pregao_class).text)
                listagem = system.clean_text(card.find('p', class_=listagem_class).text)

                if listagem:
                    for abbr, full_name in settings.governance_levels.items():
                        new_listagem = system.clean_text(listagem.replace(abbr, full_name))
                        if new_listagem != listagem:
                            listagem = new_listagem
                            break

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
    Retrieves raw HTML code from the B3 website.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    - url (str): URL of the B3 companies page.

    Returns:
    list: A list of raw HTML strings.
    """
    try:
        select_page_xpath = '//*[@id="selectPage"]'
        pagination_xpath = '//*[@id="listing_pagination"]/pagination-template/ul'
        nav_bloc_xpath = '//*[@id="nav-bloco"]/div'
        next_page_xpath = '//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a'

        driver.get(url)
        batch = system.choose(select_page_xpath, driver, driver_wait)

        text = system.text(pagination_xpath, driver_wait)
        pages = list(map(int, re.findall(r'\d+', text)))
        total_pages = max(pages) - 1

        raw_code = []
        start_time = time.time()  # Start time is relevant for the loop below

        for i, page in enumerate(range(0, total_pages + 1)):
            system.wait_forever(driver_wait, nav_bloc_xpath)
            inner_html = system.raw_text(nav_bloc_xpath, driver_wait)
            raw_code.append(inner_html)

            if i != total_pages:
                system.click(next_page_xpath, driver_wait)
            
            extra_info = [f'page {page + 1}']
            system.print_info(i, extra_info, start_time, total_pages + 1)

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

        base_name, ext = os.path.splitext(db_name)
        backup_name = f"{base_name} backup{ext}"
        db_path = os.path.join(settings.db_folder, db_name)
        backup_path = os.path.join(settings.db_folder, backup_name)

        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

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
    ticker_table_id = 'accordionBody2'
    cnpj_text = 'CNPJ'
    activity_text = 'Atividade Principal'
    sector_classification_text = 'Classificação Setorial'
    website_text = 'Site'
    escriturador_text = 'Escriturador'

    company_info = detail_soup.find('div', class_='card-body')

    ticker_codes = []
    isin_codes = []

    accordion_body = detail_soup.find('div', {'id': ticker_table_id})
    if accordion_body:
        rows = accordion_body.find_all('tr')
        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) > 1:
                ticker_codes.append(system.clean_text(cols[0].text))
                isin_codes.append(system.clean_text(cols[1].text))

    cnpj_element = company_info.find(text=cnpj_text)
    cnpj = re.sub(r'\D', '', cnpj_element.find_next('p', class_='card-linha').text) if cnpj_element else ''
    
    activity_element = company_info.find(text=activity_text)
    activity = activity_element.find_next('p', class_='card-linha').text if activity_element else ''
    
    sector_element = company_info.find(text=sector_classification_text)
    sector_classification = sector_element.find_next('p', class_='card-linha').text if sector_element else ''
    
    website_element = company_info.find(text=website_text)
    website = website_element.find_next('a').text if website_element else ''
    
    escriturador_element = detail_soup.find(text=escriturador_text)
    escriturador = escriturador_element.find_next('span').text.strip() if escriturador_element else ''

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
    existing_companies = get_existing_companies(settings.db_name)

    companies_to_process = {name: info for name, info in company_tickers.items() if name not in existing_companies}
    total_companies_to_process = len(companies_to_process)
    
    start_time = time.time()  # Start time is set at the beginning of the loop

    for i, (company_name, info) in enumerate(companies_to_process.items()):
        try:
            driver.get(settings.company_url)

            search_field_xpath = '//*[@id="keyword"]'
            nav_tab_content_xpath = '//*[@id="nav-tabContent"]'
            card_body_class = 'card-body'
            overview_xpath = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div[1]/div/div'

            search_field = system.wait_forever(driver_wait, search_field_xpath)
            search_field.clear()
            search_field.send_keys(company_name)
            search_field.send_keys(Keys.RETURN)

            system.wait_forever(driver_wait, nav_tab_content_xpath)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            cards = soup.find_all('div', class_=card_body_class)

            company_found = False
            for card in cards:
                card_ticker = system.clean_text(card.find('h5', class_='card-title2').text)
                if card_ticker == info['ticker']:
                    card_xpath = f'//h5[text()="{card_ticker}"]'
                    system.click(card_xpath, driver_wait)
                    
                    system.wait_forever(driver_wait, overview_xpath)

                    match = re.search(r'/main/(\d+)/', driver.current_url)
                    cvm_code = match.group(1) if match else ''
                    info['cvm_code'] = cvm_code

                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')

                    company_data = extract_company_data(detail_soup)

                    info.update(company_data)
                    company_found = True
                    break

        except Exception as e:
            system.log_error(f"Error processing company {company_name}: {e}")
            pass

        all_company_info[company_name] = info
        extra_info = [info['ticker'], info['cvm_code'], company_name]
        system.print_info(i, extra_info, start_time, total_companies_to_process)

        all_data.append({'company_name': company_name, **info})

        if (total_companies_to_process - i - 1) % (settings.batch_size // 5) == 0 or i == total_companies - 1:
            save_to_db(all_data, settings.db_name)
            all_data.clear()

    save_to_db(all_data, settings.db_name)
    print('Final save completed...')
    return all_company_info

def main(driver, driver_wait):
    """
    Main function to initiate the scraping of company information.

    Parameters:
    - driver: The Selenium WebDriver instance.
    - driver_wait: The WebDriverWait instance.
    
    Returns:
    dict: A dictionary with detailed company information.
    """
    raw_code = get_raw_code(driver, driver_wait, settings.companies_url)
    company_tickers = get_company_ticker(raw_code)
    company_info = get_company_info(driver, driver_wait, company_tickers)

    return company_info

if __name__ == "__main__":
    print('This is a module, not meant to be run directly.')
