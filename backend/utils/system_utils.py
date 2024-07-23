import inspect
import winsound
import time

def log_error(e):
    """
    Logs an error with the current function name.
    
    Parameters:
    - e (Exception): The exception to log.
    """
    print(f"Error in {inspect.currentframe().f_back.f_code.co_name}: {e}")


def winbeep(frequency=5000, duration=50):
    """
    Generate a system beep sound with the specified frequency and duration.

    Args:
        frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
        duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

    Returns:
        bool: True if the beep was successful, False otherwise.
    """
    winsound.Beep(frequency, duration)
    return True


def print_info(i, start, end, start_time, data):
    """
    Prints the provided information along with progress and remaining time.

    Parameters:
    - i (int): The current nsd value.
    - start (int): The start value of the current batch.
    - end (int): The end value of the current batch.
    - data (tuple): The extracted data tuple containing multiple values.
    - start_time (float): The start time of the process.
    - size (int): The total number of items to process.
    """
    size = end - start

    # Calculate remaining time and progress
    counter = i + 1
    remaining_items = size - counter
    items = (counter - start)
    
    # Calculate the percentage of completion
    percentage = items / size
    
    # Calculate the elapsed time
    running_time = time.time() - start_time
    
    # Calculate the average time taken per item
    avg_time_per_item = running_time / items
    
    # Calculate the remaining time based on the average time per item
    remaining_time = remaining_items * avg_time_per_item
    
    # Convert remaining time to hours, minutes, and seconds
    hours, remainder = divmod(int(remaining_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # Format remaining time as a string
    remaining_time_formatted = f'{int(hours)}h {int(minutes):02}m {int(seconds):02}s'
    
    # Create a progress string with all the calculated values
    progress = (
        f'{percentage:.2%} '
        f'{counter}+{remaining_items}, '
        f'{avg_time_per_item:.6f}s per item, '
        f'Remaining: {remaining_time_formatted}'
    )
    
    # Print the information
    extra_info = " ".join(map(str, data))
    print(f"{progress} {extra_info}")
    
    winbeep()