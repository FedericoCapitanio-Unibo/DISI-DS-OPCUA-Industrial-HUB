
### FILE PER LE FUNZIONI DI LOGGING

import logging
import sys
from pathlib import Path
from pythonjsonlogger import jsonlogger



def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Path | None = None,
    json_format: bool = True
) -> logging.Logger:
    """
    configrua e restituisce un logger con formatter appropriato.
    
    args:
        name: nome del logger
        level: livello di logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: path opzionale per salvare log su file
        json_format: se True usa formato JSON, altrimenti formato classico
    
    returns:
        il logger
    """
    logger = logging.getLogger(name)
    
    # evita duplicazione handler se logger già configurato
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(getattr(logging, level.upper()))
    
    # handler per stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    
    # formato dei log
    if json_format:
        # formato JSON strutturato per produzione
        formatter = jsonlogger.JsonFormatter(
            "%(timestamp)s %(level)s %(name)s %(message)s",
            rename_fields={"levelname": "level", "asctime": "timestamp"}
        )
    else:
        # formato leggibile per sviluppo
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # handler opzionale per file
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str, level: str = "INFO", json_format: bool = False) -> logging.Logger:
    """
    ottiene un logger esistente o ne crea uno
    
    args:
        name: nome del logger
    
    returns:
        il logger stesso
    """
    logger = logging.getLogger(name)
    
    #se non ha handler
    if not logger.hasHandlers():
        return setup_logger(name, level=level, json_format=json_format)
    
    return logger