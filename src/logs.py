import os
import logging
from colorama import init
from datetime import datetime
from logging.handlers import RotatingFileHandler

init()


class colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    PURPLE = "\033[95m"
    CYAN = "\033[96m"


class CustomFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': colors.RESET,
        'INFO': colors.GREEN,
        'WARNING': colors.YELLOW,
        'ERROR': colors.RED,
        'CRITICAL': colors.PURPLE
    }

    def format(self, record):
        format_orig = f"[%(asctime)s] [%(levelname)s] %(message)s"
        formatter = logging.Formatter(format_orig, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)


def cleanup_old_logs(log_directory, max_files=10):
    """
    Remove old log files, keeping only the most recent `max_files` log files.
    Args:
    - log_directory: Directory where log files are stored
    - max_files: Maximum number of recent log files to keep
    """
    log_files = sorted(
        (os.path.join(log_directory, f) for f in os.listdir(log_directory) if f.endswith('.log')),
        key=os.path.getmtime,
        reverse=True
    )
    for old_log in log_files[max_files:]:
        os.remove(old_log)


def setup_logging(base_dir, level=logging.DEBUG):
    """
    Set up logging configuration to create a new log file each execution with current datetime.
    Args:
    - base_dir: Base directory to store log files
    - level: Logging level, e.g., logging.INFO, logging.DEBUG
    """
    # Create a timestamp string to append to the log filename
    timestamp = datetime.now().strftime("[%Y-%m-%d] [%H:%M:%S]")
    log_file_name = f"{timestamp}.log"

    # Combine the base directory with the log file name
    log_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), base_dir)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file_path = os.path.join(log_directory, log_file_name)

    # Cleanup old log files
    cleanup_old_logs(log_directory)

    # Create a logger
    logger = logging.getLogger()
    logger.handlers = []  # Clear existing handlers
    logger.setLevel(level)
    logger.propagate = False  # Avoid duplicate logging

    # Setup file handler
    file_handler = RotatingFileHandler(log_file_path, maxBytes=1048576, backupCount=5)
    file_handler.setLevel(level)
    file_handler.setFormatter(CustomFormatter())
    logger.addHandler(file_handler)

    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)

    return logger
