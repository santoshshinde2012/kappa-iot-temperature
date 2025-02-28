import logging
import os
from pathlib import Path


def get_logger(name, level="INFO", file="logs/app.log"):
    logger = logging.getLogger(name)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Ensure log directory exists relative to project root
        log_path = Path(__file__).resolve().parent.parent.parent / file
        log_dir = log_path.parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger
