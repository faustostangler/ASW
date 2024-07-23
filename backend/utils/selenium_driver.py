import os
import re
import requests
import subprocess
import zipfile
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException

from utils import system
from config import settings

def get_chromedriver_path():
    """
    Defines the base path where the chromedriver will be downloaded,
    constructs the URL for the ChromeDriver based on the Chrome version,
    creates the directory if it does not exist, and
    downloads and extracts ChromeDriver to the specified directory.
    
    Returns:
    str: Path to the ChromeDriver executable.
    """
    try:
        # Define the base path where the chromedriver will be downloaded
        base_path = Path(__file__).resolve().parent.parent
        path = base_path / 'bin'
        
        # Get the version of Chrome installed on the system
        chrome_version = get_chrome_version()
        
        # Construct the URL for the ChromeDriver based on the Chrome version
        chromedriver_url = get_chromedriver_url(chrome_version)
        
        # Create the directory if it does not exist
        path.mkdir(parents=True, exist_ok=True)
        
        # Download and extract ChromeDriver to the specified directory
        chromedriver_path = download_and_extract_chromedriver(chromedriver_url, path)
        return chromedriver_path
    except Exception as e:
        system.log_error(e)
        return None

def get_chrome_version():
    """
    Queries the Windows Registry to get the installed Chrome version.
    
    Returns:
    str: Chrome version.
    """
    try:
        # Query the Windows Registry to get the installed Chrome version
        output = subprocess.check_output(
            r'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version',
            shell=True
        )
        # Extract the version number from the registry output
        version = re.search(r'\d+\.\d+\.\d+\.\d+', output.decode('utf-8')).group(0)
        return version
    except Exception as e:
        system.log_error(e)
        return None

def get_chromedriver_url(version):
    """
    Constructs the URL for downloading the ChromeDriver based on the Chrome version.
    
    Parameters:
    - version (str): Chrome version.
    
    Returns:
    str: URL to download ChromeDriver.
    """
    try:
        url = f"https://storage.googleapis.com/chrome-for-testing-public/{version}/win64/chromedriver-win64.zip"
        
        response = requests.get(url)
        if response.status_code == 200:
            return url
        else:
            print(f"Error obtaining ChromeDriver for version {version}")
            return None
    except Exception as e:
        system.log_error(e)
        return None

def download_and_extract_chromedriver(url, dest_folder):
    """
    Downloads and extracts the ChromeDriver zip file to the specified directory.
    
    Parameters:
    - url (str): URL to download ChromeDriver.
    - dest_folder (Path): Destination folder to extract ChromeDriver.
    
    Returns:
    str: Path to the extracted ChromeDriver executable.
    """
    try:
        response = requests.get(url)
        zip_path = os.path.join(dest_folder, 'chromedriver.zip')
        
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_folder)
        
        # Remove the zip file after extraction
        os.remove(zip_path)
        
        # Construct the path to the extracted ChromeDriver executable
        chromedriver_path = dest_folder / 'chromedriver-win64' / 'chromedriver.exe'
        return str(chromedriver_path.resolve())
    except Exception as e:
        system.log_error(e)
        return None

def load_driver(chromedriver_path):
    """
    Sets up the ChromeDriver service and options, and
    creates and returns a new instance of the Chrome WebDriver.
    
    Parameters:
    - chromedriver_path (str): Path to the ChromeDriver executable.
    
    Returns:
    tuple: WebDriver instance and WebDriverWait instance.
    """
    try:
        chrome_service = Service(chromedriver_path)
        chrome_options = Options()
        # Uncomment the following line to run Chrome in headless mode
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument('start-maximized')  # Maximize the window on startup.
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--log-level=ALL")

        # Create and return a new instance of the Chrome WebDriver
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        # Define the exceptions to ignore during WebDriverWait.
        exceptions_ignore = (NoSuchElementException, StaleElementReferenceException)
        
        # Create a WebDriverWait instance for the driver, using the specified wait time and exceptions to ignore.
        wait = WebDriverWait(driver, settings.driver_wait_time, ignored_exceptions=exceptions_ignore)

        return driver, wait
    except Exception as e:
        system.log_error(e)
        return None, None

def get_driver():
    """
    Defines the path to the ChromeDriver executable, and
    loads and returns the Chrome WebDriver.
    
    Returns:
    tuple: WebDriver instance and WebDriverWait instance.
    """
    try:
        # Define the path to the ChromeDriver executable
        chromedriver_path = r'D:\Fausto Stangler\Documentos\Python\ASW\backend\bin\chromedriver-win64\chromedriver.exe'
        # Alternatively, use the function to get the ChromeDriver path
        # chromedriver_path = get_chromedriver_path()
        
        # Load and return the Chrome WebDriver
        driver, wait = load_driver(chromedriver_path)
        return driver, wait
    except Exception as e:
        system.log_error(e)
        return None, None

if __name__ == "__main__":
    try:
        # Get the WebDriver instance when running the script directly
        driver = get_driver()
    except Exception as e:
        system.log_error(e)
