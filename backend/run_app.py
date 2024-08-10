
import config.settings as settings
import utils.selenium_driver as selenium_driver

if __name__ == "__main__":
    try:
        # Initialize the Selenium WebDriver
        driver, driver_wait = selenium_driver.get_driver()

    except Exception as e:
        # Log any exceptions that occur during execution
        system.log_error(e)

    # Print end message
    print('end')