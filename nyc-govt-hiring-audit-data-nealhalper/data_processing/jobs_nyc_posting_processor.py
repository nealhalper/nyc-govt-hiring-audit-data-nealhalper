import polars as pl
import pandas as pd
import sys
import os
from typing import Optional
from pathlib import Path
from datetime import datetime, timedelta
from difflib import SequenceMatcher

# Add parent directory to path to import from root level
sys.path.append(str(Path(__file__).parent.parent))

from base_pipeline import BaseDataPipeline
from config import Config
from data_acquisition.jobs_nyc_posting import NYCJobsPostingDataFetcher

class JobsPostingProcessor(BaseDataPipeline):
    def __init__(self):
        super().__init__("jobs_posting_processor")
        
        # Define the columns we want to keep
        self.relevant_columns = [
            'job_id',
            'agency',
            'posting_type',
            'number_of_positions',
            'business_title',
            'civil_service_title',
            'title_classification',
            'job_category',
            'full_time_part_time_indicator',
            'career_level',
            'salary_range_from',
            'salary_range_to',
            'salary_frequency',
            'work_location',
            'job_description',
            'minimum_qual_requirements',
            'preferred_skills',
            'posting_date',
            'posting_updated',
            'process_date'
        ]
    
    def filter_by_date(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter jobs posting data for 2024 and 2025"""
        
        print("Filtering data for 2024 and 2025...")
        print(f"Original shape: {df.shape}")
        
        # Convert posting_date to datetime if it's not already
        df_filtered = df.with_columns([
            pl.col("posting_date").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("posting_date_parsed")
        ])
        
        # Filter for 2024 and 2025
        df_filtered = df_filtered.filter(
            (pl.col("posting_date_parsed").dt.year() == 2024) | 
            (pl.col("posting_date_parsed").dt.year() == 2025)
        )
        
        print(f"Filtered shape: {df_filtered.shape}")
        print(f"Removed {df.shape[0] - df_filtered.shape[0]} records outside 2024-2025")
        
        return df_filtered
    
    def calculate_posting_duration(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculate posting duration using posting_date and process_date/posting_updated"""
        
        print("Calculating posting duration...")
        
        # Parse dates
        df_processed = df.with_columns([
            pl.col("posting_date").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("posting_start"),
            pl.col("process_date").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("posting_end"),
            pl.col("posting_updated").str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).alias("posting_updated_parsed")
        ])
        
        # Calculate posting duration
        df_processed = df_processed.with_columns([
            # Use process_date as end date, if null use posting_updated, if still null assume 30 days
            pl.when(pl.col("posting_end").is_not_null())
            .then(pl.col("posting_end"))
            .when(pl.col("posting_updated_parsed").is_not_null())
            .then(pl.col("posting_updated_parsed"))
            .otherwise(pl.col("posting_start") + pl.duration(days=30))
            .alias("calculated_end_date")
        ]).with_columns([
            # Calculate duration in days
            (pl.col("calculated_end_date") - pl.col("posting_start")).dt.total_days().alias("posting_duration_days")
        ])
        
        print("Posting duration calculation complete")
        
        # Show some stats
        duration_stats = df_processed.select([
            pl.col("posting_duration_days").mean().alias("avg_duration"),
            pl.col("posting_duration_days").median().alias("median_duration"),
            pl.col("posting_duration_days").min().alias("min_duration"),
            pl.col("posting_duration_days").max().alias("max_duration"),
            pl.col("posting_end").is_null().sum().alias("null_process_dates")
        ])
        
        print("Duration statistics:")
        print(duration_stats)
        
        return df_processed
    
    def create_fuzzy_matching_string(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create fuzzy matching string from business title and agency"""
        
        print("Creating fuzzy matching strings...")
        
        df_processed = df.with_columns([
            # Clean and normalize agency and business title
            pl.col("agency").str.to_lowercase().str.strip_chars().alias("agency_clean"),
            pl.col("business_title").str.to_lowercase().str.strip_chars().alias("business_title_clean")
        ]).with_columns([
            # Create fuzzy matching string (similar to payroll format)
            (pl.col("agency_clean") + "_" + pl.col("business_title_clean"))
            .str.replace_all(r'[^\w\s]', '', literal=False)  # Remove special characters
            .str.replace_all(r'\s+', '_', literal=False)     # Replace spaces with underscores
            .alias("fuzzy_match_string")
        ])
        
        print("Fuzzy matching strings created")
        return df_processed
    
    def calculate_fuzzy_matches_with_payroll(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate fuzzy match scores with payroll data and include salary min/max for high-quality matches
        Note: This assumes payroll processed data exists
        """
        
        print("Calculating fuzzy matches with payroll data...")
        
        # Try to load processed payroll data
        payroll_files = list(self.config.PROCESSED_DATA_DIRECTORY.glob("nyc_payroll_processed_*.parquet"))
        
        if not payroll_files:
            print("Warning: No processed payroll data found. Skipping fuzzy matching with payroll.")
            return df.with_columns([
                pl.lit(None).alias("best_payroll_match"),
                pl.lit(None).alias("best_match_score"),
                pl.lit(None).alias("payroll_salary_min"),
                pl.lit(None).alias("payroll_salary_max")
            ])
        
        # Load the most recent payroll processed file
        latest_payroll_file = sorted(payroll_files)[-1]
        print(f"Loading payroll data from: {latest_payroll_file}")
        
        try:
            payroll_df = pl.read_parquet(latest_payroll_file)
            
            # Create a dictionary for fast lookup of salary data by fuzzy match string
            payroll_salary_lookup = {}
            for row in payroll_df.iter_rows(named=True):
                fuzzy_string = row.get('fuzzy_match_string')
                salary_min = row.get('salary_min')
                salary_max = row.get('salary_max')
                
                if fuzzy_string and salary_min is not None and salary_max is not None:
                    # Store min/max salary for this fuzzy string (use average if multiple entries)
                    if fuzzy_string not in payroll_salary_lookup:
                        payroll_salary_lookup[fuzzy_string] = {'salaries_min': [], 'salaries_max': []}
                    payroll_salary_lookup[fuzzy_string]['salaries_min'].append(salary_min)
                    payroll_salary_lookup[fuzzy_string]['salaries_max'].append(salary_max)
            
            # Calculate average salaries for each fuzzy string
            for fuzzy_string in payroll_salary_lookup:
                min_salaries = payroll_salary_lookup[fuzzy_string]['salaries_min']
                max_salaries = payroll_salary_lookup[fuzzy_string]['salaries_max']
                payroll_salary_lookup[fuzzy_string] = {
                    'avg_min': sum(min_salaries) / len(min_salaries),
                    'avg_max': sum(max_salaries) / len(max_salaries)
                }
            
            payroll_fuzzy_strings = list(payroll_salary_lookup.keys())
            print(f"Loaded {len(payroll_fuzzy_strings)} unique payroll fuzzy match strings with salary data")
            
            # Function to find best fuzzy match with salary data
            def find_best_match_with_salary(job_fuzzy_string):
                if not job_fuzzy_string or job_fuzzy_string is None:
                    return None, None, None, None
                
                best_match = None
                best_score = 0.0
                best_salary_min = None
                best_salary_max = None
                
                for payroll_string in payroll_fuzzy_strings:
                    if payroll_string:
                        score = SequenceMatcher(None, job_fuzzy_string, payroll_string).ratio()
                        if score > best_score:
                            best_score = score
                            best_match = payroll_string
                
                # Only return match data if score >= 0.85
                if best_score >= 0.85:
                    salary_data = payroll_salary_lookup[best_match]
                    return best_match, best_score, salary_data['avg_min'], salary_data['avg_max']
                else:
                    return None, None, None, None
            
            # Apply fuzzy matching (this might be slow for large datasets)
            print("Computing fuzzy matches with salary data (this may take a while)...")
            
            matches_scores_salaries = [find_best_match_with_salary(fs) for fs in df.select("fuzzy_match_string").to_series().to_list()]
            
            matches = [m[0] for m in matches_scores_salaries]
            scores = [m[1] for m in matches_scores_salaries]
            salary_mins = [m[2] for m in matches_scores_salaries]
            salary_maxs = [m[3] for m in matches_scores_salaries]
            
            # Add results to dataframe
            df_with_matches = df.with_columns([
                pl.Series("best_payroll_match", matches),
                pl.Series("best_match_score", scores),
                pl.Series("payroll_salary_min", salary_mins),
                pl.Series("payroll_salary_max", salary_maxs)
            ])
            
            # Show match quality statistics
            match_stats = df_with_matches.select([
                pl.col("best_match_score").mean().alias("avg_match_score"),
                pl.col("best_match_score").median().alias("median_match_score"),
                pl.col("best_match_score").is_not_null().sum().alias("high_quality_matches_85_plus"),
                pl.col("best_match_score").count().alias("total_jobs"),
                pl.col("payroll_salary_min").is_not_null().sum().alias("jobs_with_payroll_salary")
            ])
            
            print("Fuzzy match statistics:")
            print(match_stats)
            
            return df_with_matches
            
        except Exception as e:
            print(f"Error during fuzzy matching: {e}")
            return df.with_columns([
                pl.lit(None).alias("best_payroll_match"),
                pl.lit(None).alias("best_match_score"),
                pl.lit(None).alias("payroll_salary_min"),
                pl.lit(None).alias("payroll_salary_max")
            ])
    
    def select_and_process_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Select relevant columns and process the data"""
        
        print(f"Original DataFrame shape: {df.shape}")
        print(f"Original columns: {list(df.columns)}")
        
        # Check which of our desired columns exist in the data
        available_columns = [col for col in self.relevant_columns if col in df.columns]
        missing_columns = [col for col in self.relevant_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Warning: Missing columns: {missing_columns}")
        
        print(f"Selecting columns: {available_columns}")
        
        # Select only the relevant columns that exist
        df_selected = df.select(available_columns)
        
        # Apply all processing steps
        df_processed = self.filter_by_date(df_selected)
        df_processed = self.calculate_posting_duration(df_processed)
        df_processed = self.create_fuzzy_matching_string(df_processed)
        df_processed = self.calculate_fuzzy_matches_with_payroll(df_processed)
        
        # Select final columns for output (UPDATED: Added payroll salary columns)
        final_columns = [
            'job_id',
            'agency', 
            'posting_type',
            'number_of_positions',
            'business_title',
            'civil_service_title',
            'job_category',
            'full_time_part_time_indicator',
            'career_level',
            'salary_range_from',
            'salary_range_to',
            'salary_frequency',
            'work_location',
            'posting_date',
            'posting_duration_days',
            'fuzzy_match_string',
            'best_payroll_match',
            'best_match_score',
            'payroll_salary_min',  # NEW
            'payroll_salary_max'   # NEW
        ]
        
        # Only select columns that exist
        final_columns = [col for col in final_columns if col in df_processed.columns]
        df_final = df_processed.select(final_columns)
        
        print(f"Processed DataFrame shape: {df_final.shape}")
        print(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def process_jobs_posting_data(self, input_df: Optional[pl.DataFrame] = None, use_cache: bool = True) -> pl.DataFrame:
        """
        Main processing function for jobs posting data
        
        Args:
            input_df: Optional DataFrame to process (if None, will fetch fresh data)
            use_cache: Whether to use cached data for fetching
        """
        
        if input_df is None:
            print("No input DataFrame provided. Fetching fresh data...")
            fetcher = NYCJobsPostingDataFetcher()
            input_df = fetcher.fetch_data(max_rows=Config.DEVELOPMENT_ROW_LIMIT, use_cache=use_cache)
        
        if input_df.is_empty():
            print("No data to process")
            return pl.DataFrame()
        
        print("="*60)
        print("PROCESSING JOBS POSTING DATA")
        print("="*60)
        
        # Process the data
        processed_df = self.select_and_process_columns(input_df)
        
        # Save processed data
        timestamp = Config.get_timestamp()
        processed_file = self.save_processed_data(processed_df, f"nyc_jobs_posting_processed_{timestamp}")
        
        # Create processing metadata
        processing_metadata = {
            'processing_type': 'jobs_posting_processing',
            'input_rows': input_df.shape[0],
            'output_rows': processed_df.shape[0],
            'columns_selected': list(processed_df.columns),
            'transformations': [
                'date_filtering_2024_2025',
                'posting_duration_calculation',
                'fuzzy_matching_string_creation',
                'conditional_payroll_fuzzy_matching_85_percent_threshold'
            ],
            'match_threshold': 0.85,
            'match_threshold_description': 'Only include payroll match data (match string, score, salary min/max) when similarity >= 85%',
            'processed_timestamp': Config.get_timestamp()
        }
        
        self.save_metadata(processing_metadata, f"jobs_posting_processing_{timestamp}")
        
        print(f"\n✅ Processing complete!")
        print(f"Processed data saved to: {processed_file}")
        print(f"Sample of processed data:")
        print(processed_df.head())
        
        return processed_df

def main():
    """Main function to process jobs posting data"""
    processor = JobsPostingProcessor()
    
    print("=== JOBS POSTING DATA PROCESSING ===")
    processed_df = processor.process_jobs_posting_data(use_cache=True)
    
    if not processed_df.is_empty():
        print(f"\nFinal processed dataset shape: {processed_df.shape}")
        print("\nSample posting durations:")
        print(processed_df.select(['business_title', 'posting_date', 'posting_duration_days']).head())
        
        print("\nSample fuzzy matches with payroll (only showing matches >= 85%):")
        print(processed_df.select(['business_title', 'fuzzy_match_string', 'best_payroll_match', 'best_match_score', 'payroll_salary_min', 'payroll_salary_max']).head())
        
        # Show high-quality matches summary
        high_quality_matches = processed_df.filter(pl.col("best_match_score").is_not_null())
        if not high_quality_matches.is_empty():
            print(f"\nHigh-quality matches (>= 85%): {high_quality_matches.shape[0]} jobs")
            print("Sample high-quality matches with salary data:")
            print(high_quality_matches.select(['business_title', 'best_match_score', 'payroll_salary_min', 'payroll_salary_max']).head())
        else:
            print(f"\nNo high-quality matches (>= 85%) found in this dataset.")

if __name__ == "__main__":
    main()