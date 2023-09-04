import logging

def setup_logger(filename,level) -> logging.RootLogger:
    """instance of logger module, will be used for logging operations"""
    
    # logger config
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    # log format
    lg_format = "%(levelname)s|%(filename)s:%(lineno)d|%(asctime)s|%(message)s"
    log_format = logging.Formatter(lg_format)

    # file handler
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(log_format)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    return logger
