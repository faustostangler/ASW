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

        # # Ask the user if they want to scrape company information
        # scrape_choice = input("Want to scrape company information? (YES/NO): ")
        scrape_choice = 'N'
        if scrape_choice.strip().upper().startswith('Y'):
            company_info = company_scrap.main(driver, driver_wait)

        # # Ask the user if they want to scrape company information
        # nsd_choice = input("Want to update the NSD list? (YES/NO): ")
        nsd_choice = 'Y'
        if nsd_choice.strip().upper().startswith('Y'):
            # Scrape NSD values
            df = nsd_scrap.main()

        # Scrape Financial Sheets
        # nsd_list_choice = input("Want to download new NSD items from the NSD list? (YES/NO): ")
        nsd_list_choice = 'Y'
        if nsd_list_choice.strip().upper().startswith('Y'):
            all_math = finsheet_scrap.main(driver, driver_wait, batch_size=settings.big_batch_size, batch=None)

        # Re-calculate finsheet
        fin_math = finsheet_math.main()

        # Close the browser window
        driver.quit()

    except Exception as e:
        # Log any exceptions that occur during execution
        system.log_error(e)

    # Print end message
    print('end')
