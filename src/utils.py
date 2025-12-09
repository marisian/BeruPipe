import pandas as pd
import re
import ast
import logging
from pathlib import Path

def start_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
        )
    logger = logging.getLogger(__name__)
    return logger
                    
    
    
    