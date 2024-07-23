from pathlib import Path
import sys

# Add the 'scripts' directory path to sys.path so we can import scripts as a package
scripts_path = Path(__file__).resolve().parent / 'scripts'
sys.path.append(str(scripts_path))

# Import the combined module
from config import settings
from utils import selenium_driver
from utils import nsd_scrap
from utils import company_scrap
from utils import system
from utils import fintastix

if __name__ == "__main__":
    try:
        # Initialize the Selenium WebDriver
        driver, driver_wait = selenium_driver.get_driver()

        # companies info scraping
        raw_code = company_scrap.get_raw_code(driver, driver_wait, settings.companies_url)
        company_tickers = company_scrap.get_company_ticker(raw_code)

        # NSD values scraping
        nsd_scrap.scrape_nsd_values(driver, driver_wait, 'b3.db')

        # Close the browser window
        driver.quit()

    except Exception as e:
        system.log_error(e)

    print('end')