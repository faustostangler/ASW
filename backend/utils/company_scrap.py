import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException

from utils import system
from utils import selenium_driver
from config import settings

# Function to extract tickers and company names
def get_company_ticker(raw_code):
    # Initialize a list to hold the keyword information
    companies = {}

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
                companies[company_name] = {}
                companies[company_name]['ticker'] = ticker
                # companies[company_name]['company_name'] = company_name
                companies[company_name]['pregao'] = pregao
                companies[company_name]['listagem'] = listagem
            except Exception as e:
                # print(e)
                pass

    return companies

# Function to get tickers from B3
def get_raw_code(driver, wait, url=settings.companies_url):
    try:
        # Get the total number of companies and pages
        driver.get(url)
        batch = system.choose(f'//*[@id="selectPage"]', driver, wait)
        companies = system.text(f'//*[@id="divContainerIframeB3"]/form/div[1]/div/div/div[1]/p/span[1]', wait)
        companies = int(companies.replace('.', ''))
        pages = int(companies / batch)

        value = f'found {companies} companies in {pages + 1} pages'
        print(value)

        raw_code = []
        start_time = time.time()
        for i, page in enumerate(range(0, pages + 1)):
            xpath = '//*[@id="nav-bloco"]/div'
            inner_html = system.raw_text(xpath, wait)
            raw_code.append(inner_html)
            system.click(f'//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a', wait)
            while True:
                try:
                    element = wait.until(EC.element_to_be_clickable((By.XPATH, f'//*[@id="selectPage"]')))
                    break
                except Exception as e:
                    time.sleep(settings.wait_time)  # Wait for 1 second and try again
            value = f'page {page + 1}'
            extra_info = []
            system.print_info(i, 0, pages, extra_info, start_time, pages + 1)

    except Exception as e:
        print(e)
        raw_code = []

    return raw_code

# Main script execution
if __name__ == "__main__":
    driver, driver_wait = selenium_driver.get_driver()

    raw_code = get_raw_code(driver, driver_wait, settings.companies_url)
    company_tickers = get_company_ticker(raw_code)


    driver.quit()

    # Convert to DataFrame
    companies_df = pd.DataFrame(company_tickers, columns=['ticker', 'company_name', 'pregao', 'listagem'])
    companies_df.to_csv('company_data.csv', index=False)
    print("Data saved to company_data.csv")
