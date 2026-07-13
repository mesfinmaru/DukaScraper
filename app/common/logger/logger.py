import logging
from colorama import Fore, Style, init

# Initialize colorama for Windows support
init(autoreset=True)

class CustomFormatter(logging.Formatter):
    """Custom formatting for beautiful terminal logs."""
    format_str = "%(asctime)s | %(levelname)-8s | %(message)s"
    
    FORMATS = {
        logging.DEBUG: Fore.BLUE + format_str + Style.RESET_ALL,
        logging.INFO: Fore.GREEN + format_str + Style.RESET_ALL,
        logging.WARNING: Fore.YELLOW + format_str + Style.RESET_ALL,
        logging.ERROR: Fore.RED + format_str + Style.RESET_ALL,
        logging.CRITICAL: Fore.RED + Style.BRIGHT + format_str + Style.RESET_ALL
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

# Setup Logger
logger = logging.getLogger("DukaScraper")
logger.setLevel(logging.INFO)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())

# Add handler to logger
if not logger.handlers:
    logger.addHandler(ch)