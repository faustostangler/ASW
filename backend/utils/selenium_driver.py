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
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException

from utils import system
from config import settings


def get_chromedriver_path() -> Path:
    '''
    Get the path to the ChromeDriver executable. If it doesn't exist, download and extract it.
    
    Returns:
    Path: Path to the ChromeDriver executable.
    '''
    try:
        # Define the base path where the ChromeDriver will be stored
        base_path = Path(__file__).resolve().parent.parent
        bin_path = base_path / settings.driver_folder
        
        # Ensure the directory exists
        bin_path.mkdir(parents=True, exist_ok=True)
        
        # Get the installed Chrome version
        chrome_version = get_chrome_version()
        if chrome_version is None:
            raise RuntimeError('Unable to determine Chrome version.')
        
        # Construct the download URL for ChromeDriver
        chromedriver_url = get_chromedriver_url(chrome_version)
        if chromedriver_url is None:
            raise RuntimeError(f'ChromeDriver for version {chrome_version} could not be found.')
        
        # Download and extract ChromeDriver
        chromedriver_path = download_and_extract_chromedriver(chromedriver_url, bin_path)
        return chromedriver_path

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None


def get_chrome_version() -> str:
    '''
    Retrieve the installed version of Google Chrome on Windows.
    
    Returns:
    str: The installed Chrome version.
    '''
    try:
        # Query the Windows Registry to get the installed Chrome version
        output = subprocess.check_output(
            settings.chrome_version_cmd,
            shell=True
        )
        
        # Extract the version number from the registry output
        version_match = re.search(r'\d+\.\d+\.\d+\.\d+', output.decode('utf-8'))
        return version_match.group(0) if version_match else None

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None


def get_chromedriver_url(version: str) -> str:
    '''
    Construct the download URL for ChromeDriver based on the installed Chrome version.
    
    Parameters:
    - version (str): The installed Chrome version.
    
    Returns:
    str: The URL to download ChromeDriver.
    '''
    try:
        # Construct the download URL for the appropriate ChromeDriver version
        url = f'{settings.chrome_driver_base_url}/{version}{settings.chrome_drive_system}'

        # Check if the URL is valid by making an HTTP request
        response = requests.get(url)
        if response.status_code == 200:
            return url
        else:
            system.log_error(f'Failed to obtain ChromeDriver for version {version}. HTTP status code: {response.status_code}')
            return None

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None


def download_and_extract_chromedriver(url: str, dest_folder: Path) -> Path:
    '''
    Download and extract ChromeDriver from the specified URL to the destination folder.
    
    Parameters:
    - url (str): The URL to download ChromeDriver.
    - dest_folder (Path): The folder where ChromeDriver will be extracted.
    
    Returns:
    Path: The path to the extracted ChromeDriver executable.
    '''
    try:
        # Download the ChromeDriver zip file
        response = requests.get(url)
        response.raise_for_status()
        
        # Define the path where the zip file will be saved
        zip_path = dest_folder / 'chromedriver.zip'
        
        # Save the downloaded content to the zip file
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_folder)
        
        # Remove the zip file after extraction
        os.remove(zip_path)
        
        # Path to the extracted ChromeDriver executable
        chromedriver_executable = dest_folder / 'chromedriver-win64' / 'chromedriver.exe'
        return chromedriver_executable.resolve()

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None


def load_driver(chromedriver_path: Path) -> (webdriver.Chrome, WebDriverWait): # type: ignore
    '''
    Initialize and return a Selenium WebDriver instance and WebDriverWait.
    
    Parameters:
    - chromedriver_path (Path): The path to the ChromeDriver executable.
    
    Returns:
    tuple: WebDriver instance and WebDriverWait instance.
    '''
    try:
        # Create a Service object using the ChromeDriver path
        chrome_service = Service(str(chromedriver_path))
        
        # Create an Options object to configure ChromeDriver options
        chrome_options = Options()
        
        # Add arguments to the ChromeDriver options
        for arg in settings.chrome_options_args:
            chrome_options.add_argument(arg)
        
        # Add the headless option if specified in settings
        if settings.chrome_headless:
            chrome_options.add_argument('--headless')

        # Create the WebDriver instance
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

        # Define exceptions to ignore during WebDriverWait
        exceptions_ignore = (NoSuchElementException, StaleElementReferenceException)
        
        # Create and return a WebDriverWait instance
        driver_wait = WebDriverWait(driver, settings.wait_time, ignored_exceptions=exceptions_ignore)

        return driver, driver_wait

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None, None


def get_driver() -> (webdriver.Chrome, WebDriverWait): # type: ignore
    '''
    Get a Selenium WebDriver instance. If ChromeDriver is not available, download and extract it.
    
    Returns:
    tuple: WebDriver instance and WebDriverWait instance.
    '''
    try:
        # Get the ChromeDriver path
        chromedriver_path = get_chromedriver_path()
        if chromedriver_path is None or not chromedriver_path.exists():
            raise RuntimeError('ChromeDriver could not be found or downloaded.')
        
        # Load and return the Chrome WebDriver
        driver, driver_wait = load_driver(chromedriver_path)
        return driver, driver_wait

    except Exception as e:
        # Log the error and return None if an exception occurs
        system.log_error(e)
        return None, None


if __name__ == '__main__':
    print('This is a module. Done!')
