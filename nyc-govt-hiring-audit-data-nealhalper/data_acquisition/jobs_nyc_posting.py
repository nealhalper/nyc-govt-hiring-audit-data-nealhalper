import requests
import polars as pl
import time
import os
from typing import Optional
from dotenv import load_dotenv
from base_pipeline import BaseDataPipeline
from config import Config

# Load environment variables
load_dotenv()

class NYCJobsPostingDataFetcher(BaseDataPipeline):
    def __init__(self):
        super().__init__("nyc_jobs_posting")
        self.base_url = os.getenv('NYC_JOBS_POSTING_API_URL')
        if not self.base_url:
            raise ValueError("NYC_JOBS_POSTING_API_URL not found in environment variables")
        self.default_limit = Config.DEFAULT_BATCH_SIZE
        
    def fetch_batch(self, offset: int = 0, limit: int = 1000) -> list:
        """Fetch a single batch of data from the API"""
        params = {
            '$offset': offset,
            '$limit': limit
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            print(f"Fetched {len(data)} records (offset: {offset})")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data at offset {offset}: {e}")
            return []
    
    def fetch_data(self, max_rows: Optional[int] = None, delay: float = None, use_cache: bool = True) -> pl.DataFrame:
        """
        Fetch NYC jobs posting data with caching and raw data storage
        
        Args:
            max_rows: Maximum number of rows to fetch
            delay: Delay between API calls
            use_cache: Whether to use cached raw data if available
        
        Returns:
            Raw DataFrame (unprocessed)
        """
        delay = delay or Config.API_DELAY
        
        # Check for cached data first
        if use_cache:
            cached_file = self.find_recent_raw_file(max_age_hours=24)
            if cached_file:
                print("Using cached raw data")
                raw_data = self.load_raw_data(cached_file)
                return pl.DataFrame(raw_data if isinstance(raw_data, list) else raw_data.get('data', []))
        
        # Fetch new data
        all_data = []
        offset = 0
        batch_size = self.default_limit
        
        print(f"Starting NYC Jobs Posting data fetch...")
        if max_rows:
            print(f"Limiting to first {max_rows:,} rows")
        
        while True:
            if max_rows and offset + batch_size > max_rows:
                batch_size = max_rows - offset
            
            batch_data = self.fetch_batch(offset, batch_size)
            
            if not batch_data:
                break
                
            all_data.extend(batch_data)
            offset += len(batch_data)
            
            if max_rows and len(all_data) >= max_rows:
                print(f"Reached maximum rows limit: {len(all_data):,}")
                break
            
            if len(batch_data) < batch_size:
                print(f"Reached end of data: {len(all_data):,} total records")
                break
            
            if delay > 0:
                time.sleep(delay)
        
        print(f"Total records fetched: {len(all_data):,}")
        
        # Save raw data and metadata (but don't process)
        if all_data:
            self.save_raw_data(all_data)
            
            # Create and save metadata
            metadata = {
                'source': 'NYC Jobs Posting API',
                'url': self.base_url,
                'total_records': len(all_data),
                'fetch_timestamp': Config.get_timestamp(),
                'max_rows_requested': max_rows,
                'columns': list(all_data[0].keys()) if all_data else []
            }
            self.save_metadata(metadata, f"jobs_posting_fetch_{Config.get_timestamp()}")
            
            df = pl.DataFrame(all_data)
            print(f"Raw DataFrame shape: {df.shape}")
            print("Note: Data is unprocessed. Use jobs_nyc_posting_processor.py to filter and process.")
            return df
        else:
            return pl.DataFrame()

def main():
    """Main function to fetch NYC jobs posting data (no processing)"""
    fetcher = NYCJobsPostingDataFetcher()
    
    # Development mode with caching
    print("=== FETCHING ONLY: Getting NYC Jobs Posting data ===")
    df_raw = fetcher.fetch_data(max_rows=Config.DEVELOPMENT_ROW_LIMIT, use_cache=True)
    
    if not df_raw.is_empty():
        print("\nRaw data sample:")
        print(df_raw.head())
        print(f"\nAvailable columns: {list(df_raw.columns)}")
        print("\nTo process this data, run: python data_processing/jobs_nyc_posting_processor.py")

if __name__ == "__main__":
    main()