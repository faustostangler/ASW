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
from utils import finsheet_scrap
from utils import finsheet_math
from utils import system
from utils import finsheet_scrap

if __name__ == "__main__":
    try:
        # Initialize the Selenium WebDriver
        driver, driver_wait = selenium_driver.get_driver()

        # # Scrape company information
        # raw_code = company_scrap.get_raw_code(driver, driver_wait, settings.companies_url)
        # company_tickers = company_scrap.get_company_ticker(raw_code)
        # company_info = company_scrap.get_company_info(driver, driver_wait, company_tickers)

        # # Scrape NSD values
        # nsd_scrap.main(settings.db_name)

        # # Scrape Financial Sheets
        # nsd_list = finsheet_scrap.main(driver, driver_wait, batch_size=settings.big_batch_size, batch=2)

        # Re-calculate finsheet
        fin_math = finsheet_math.main()

        # Close the browser window
        driver.quit()

    except Exception as e:
        # Log any exceptions that occur during execution
        system.log_error(e)

    # Print end message
    print('end')
