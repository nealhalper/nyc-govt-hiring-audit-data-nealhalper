import os
from datetime import datetime
from pathlib import Path

class Config:
    # Base directories
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    
    # Raw data storage
    RAW_DATA_DIRECTORY = DATA_DIR / "raw"
    PROCESSED_DATA_DIRECTORY = DATA_DIR / "processed" 
    METADATA_DIRECTORY = DATA_DIR / "metadata"
    
    # API Configuration
    API_DELAY = 0.1  # seconds between API calls
    DEFAULT_BATCH_SIZE = 1000
    DEVELOPMENT_ROW_LIMIT = 10000
    
    # File naming
    TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
    
    @classmethod
    def ensure_directories(cls):
        """Create all necessary directories"""
        for directory in [cls.RAW_DATA_DIRECTORY, cls.PROCESSED_DATA_DIRECTORY, cls.METADATA_DIRECTORY]:
            directory.mkdir(parents=True, exist_ok=True)
            
    @classmethod
    def get_timestamp(cls):
        """Get current timestamp string"""
        return datetime.now().strftime(cls.TIMESTAMP_FORMAT)

# Initialize directories when config is imported
Config.ensure_directories()