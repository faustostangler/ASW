from pathlib import Path
import sys

# Add the 'scripts' directory path to sys.path so we can import scripts as a package
scripts_path = Path(__file__).resolve().parent / 'scripts'
sys.path.append(str(scripts_path))

# Import the combined module
from config import settings as setup
from utils import selenium_driver as drv
from utils import nsd_scrap as nsd
from utils import nsd_manager as mgr
from utils import system_utils
from utils import fintastix


if __name__ == "__main__":
    try:
        # Initialize the Selenium WebDriver
        driver, wait = drv.get_driver()

        # Start scraping
        db_name = 'nsd.db'

        # missing_nsds = mgr.manage_nsd(db_name)

        nsd.nsd_scrape(driver, wait)
        

        # fintastix.main_scrape_process(driver, wait)

        # Close the browser window
        driver.quit()

        print('done')
    except Exception as e:
        system_utils.log_error(e)
