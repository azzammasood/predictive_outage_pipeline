import logging
import colorlog

# Function to get logger
def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        handler = colorlog.StreamHandler()
        handler.setFormatter(colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='[%Y-%m-%d %H:%M:%S]'
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
