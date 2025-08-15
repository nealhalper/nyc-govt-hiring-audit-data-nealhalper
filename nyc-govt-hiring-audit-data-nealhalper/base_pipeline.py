import json
import polars as pl
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from config import Config

class BaseDataPipeline:
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.config = Config
        
    def save_raw_data(self, data: Any, suffix: str = "") -> Path:
        """Save raw data with timestamp"""
        timestamp = self.config.get_timestamp()
        filename = f"{self.source_name}_{timestamp}{suffix}.json"
        filepath = self.config.RAW_DATA_DIRECTORY / filename
        
        with open(filepath, 'w') as f:
            if isinstance(data, (dict, list)):
                json.dump(data, f, indent=2)
            else:
                json.dump({"data": str(data)}, f, indent=2)
        
        print(f"Raw data saved: {filepath}")
        return filepath
    
    def load_raw_data(self, filepath: Path) -> Dict[str, Any]:
        """Load previously saved raw data"""
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def find_recent_raw_file(self, max_age_hours: int = 24) -> Optional[Path]:
        """Find recent raw data file within max_age_hours"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        pattern = f"{self.source_name}_*.json"
        matching_files = list(self.config.RAW_DATA_DIRECTORY.glob(pattern))
        
        for filepath in sorted(matching_files, reverse=True):  # Most recent first
            # Extract timestamp from filename
            try:
                timestamp_str = filepath.stem.split('_', 1)[1].split('_')[0] + '_' + filepath.stem.split('_', 1)[1].split('_')[1]
                file_time = datetime.strptime(timestamp_str, self.config.TIMESTAMP_FORMAT)
                
                if file_time > cutoff_time:
                    print(f"Found recent raw data: {filepath}")
                    return filepath
            except (ValueError, IndexError):
                continue  # Skip files that don't match expected format
                
        return None
    
    def save_processed_data(self, df: pl.DataFrame, filename: str) -> Path:
        """Save processed data as parquet"""
        filepath = self.config.PROCESSED_DATA_DIRECTORY / f"{filename}.parquet"
        df.write_parquet(filepath)
        print(f"Processed data saved: {filepath}")
        return filepath
    
    def save_metadata(self, metadata: Dict[str, Any], filename: str) -> Path:
        """Save processing metadata"""
        filepath = self.config.METADATA_DIRECTORY / f"{filename}.json"
        metadata['processed_timestamp'] = datetime.now().isoformat()
        
        with open(filepath, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Metadata saved: {filepath}")
        return filepath