import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

VERBOSE = True
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')

def get_logger(name: str = 'acquisition-util'):
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(LOG_LEVEL)
        
        detailed_format = '[%(levelname)-7s] [%(asctime)s - %(module)s.%(funcName)s:%(lineno)03d] %(message)s'
        simple_format = '[%(asctime)s] %(message)s'
        
        log_format = detailed_format if VERBOSE else simple_format
        date_format = '%Y-%m-%d %H:%M:%S'
        
        formatter = logging.Formatter(log_format, datefmt=date_format)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        file_handler = logging.FileHandler(f'logs/{name}.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

log = get_logger()
