from data_acquisition.citywide_payroll_data import NYCPayrollDataFetcher
from data_acquisition.jobs_nyc_posting import NYCJobsPostingDataFetcher
from data_acquisition.lighthouse_data import LighthouseDataProcessor
from data_processing.citywide_payroll_processor import PayrollDataProcessor
from data_processing.jobs_nyc_posting_processor import JobsPostingProcessor
from config import Config

def run_development_pipeline():
    """Run all data fetchers and processors in development mode"""
    print("="*60)
    print("RUNNING DEVELOPMENT DATA PIPELINE")
    print("="*60)
    
    # 1. Fetch NYC Payroll Data (raw)
    print("\n1. Fetching NYC Payroll Data...")
    payroll_fetcher = NYCPayrollDataFetcher()
    payroll_raw_df = payroll_fetcher.fetch_data(
        max_rows=Config.DEVELOPMENT_ROW_LIMIT, 
        use_cache=True
    )
    
    # 2. Process NYC Payroll Data
    print("\n2. Processing NYC Payroll Data...")
    payroll_processor = PayrollDataProcessor()
    payroll_processed_df = payroll_processor.process_payroll_data(
        input_df=payroll_raw_df,
        use_cache=True
    )
    
    # 3. Fetch NYC Jobs Posting Data  
    print("\n3. Fetching NYC Jobs Posting Data...")
    jobs_fetcher = NYCJobsPostingDataFetcher()
    jobs_raw_df = jobs_fetcher.fetch_data(
        max_rows=Config.DEVELOPMENT_ROW_LIMIT,
        use_cache=True
    )
    
    # 4. Process NYC Jobs Posting Data
    print("\n4. Processing NYC Jobs Posting Data...")
    jobs_processor = JobsPostingProcessor()
    jobs_processed_df = jobs_processor.process_jobs_posting_data(
        input_df=jobs_raw_df,
        use_cache=True
    )
    
    # 5. Process Lighthouse Data
    print("\n5. Processing Lighthouse Data...")
    lighthouse_processor = LighthouseDataProcessor()
    lighthouse_processor.process_excel_file(use_cache=True)
    
    print("\n" + "="*60)
    print("PIPELINE COMPLETE - Check ./data/ directories for results")
    print("Raw data: ./data/raw/")
    print("Processed data: ./data/processed/") 
    print("Metadata: ./data/metadata/")
    print("="*60)

if __name__ == "__main__":
    run_development_pipeline()