import polars as pl
import sys
import os
from typing import Optional
from pathlib import Path

# Add parent directory to path to import from root level
sys.path.append(str(Path(__file__).parent.parent))

from base_pipeline import BaseDataPipeline
from config import Config
from data_acquisition.citywide_payroll_data import NYCPayrollDataFetcher

class PayrollDataProcessor(BaseDataPipeline):
    def __init__(self):
        super().__init__("payroll_processor")
        
        # Define the columns we want to keep
        self.relevant_columns = [
            'agency_name',
            'title_description',  # Title Description (same as job title in this dataset)
            'base_salary',
            'pay_basis',
            'regular_hours',
            'regular_gross_paid',
            'ot_hours',
            'total_ot_paid',
            'total_other_pay'
        ]
    
    def standardize_annual_salary(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create a standardized annual salary column"""
        
        print("Creating standardized annual salary column...")
        
        df_processed = df.with_columns([
            # Convert salary columns to numeric, handling any string values
            pl.col("base_salary").cast(pl.Float64, strict=False).alias("base_salary_num"),
            pl.col("regular_gross_paid").cast(pl.Float64, strict=False).alias("regular_gross_num"),
            pl.col("total_ot_paid").cast(pl.Float64, strict=False).alias("total_ot_num"),
            pl.col("total_other_pay").cast(pl.Float64, strict=False).alias("total_other_num"),
            pl.col("regular_hours").cast(pl.Float64, strict=False).alias("regular_hours_num")
        ])
        
        # Create annual salary based on pay basis
        df_processed = df_processed.with_columns([
            pl.when(pl.col("pay_basis") == "per Annum")
            .then(pl.col("base_salary_num"))
            .when(pl.col("pay_basis") == "per Hour")
            .then(pl.col("base_salary_num") * 2080)  # Standard full-time hours per year
            .when(pl.col("pay_basis") == "per Day")
            .then(pl.col("base_salary_num") * 260)   # Standard work days per year
            .otherwise(
                # If unclear, use total compensation
                pl.col("regular_gross_num") + 
                pl.col("total_ot_num").fill_null(0) + 
                pl.col("total_other_num").fill_null(0)
            )
            .alias("annual_salary_standardized")
        ])
        
        print(f"Annual salary standardization complete")
        return df_processed
    
    def create_salary_min_max(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create salary min/max columns by grouping by agency_name and title_description"""
        
        print("Creating salary min/max columns by grouping agency and title...")
        
        # First, ensure base_salary_num exists from the standardize_annual_salary step
        if 'base_salary_num' not in df.columns:
            print("Warning: base_salary_num column not found. Running salary standardization first...")
            df = self.standardize_annual_salary(df)
        
        # Group by agency_name and title_description to find min/max base salary
        salary_ranges = df.group_by(['agency_name', 'title_description']).agg([
            pl.col('base_salary_num').min().alias('salary_min'),
            pl.col('base_salary_num').max().alias('salary_max'),
            pl.col('base_salary_num').count().alias('employee_count_in_role')
        ])
        
        print(f"Created salary ranges for {salary_ranges.shape[0]} unique agency-title combinations")
        
        # Join the salary ranges back to the original dataframe
        df_processed = df.join(
            salary_ranges, 
            on=['agency_name', 'title_description'], 
            how='left'
        )
        
        # Calculate salary range width
        df_processed = df_processed.with_columns([
            (pl.col("salary_max") - pl.col("salary_min")).alias("salary_range_width")
        ])
        
        # Show some statistics
        range_stats = df_processed.select([
            pl.col("salary_min").mean().alias("avg_min_salary"),
            pl.col("salary_max").mean().alias("avg_max_salary"), 
            pl.col("salary_range_width").mean().alias("avg_range_width"),
            (pl.col("salary_range_width") > 0).sum().alias("positions_with_salary_range"),
            pl.col("employee_count_in_role").mean().alias("avg_employees_per_role")
        ])
        
        print("Salary range statistics (grouped by agency and title):")
        print(range_stats)
        
        # Show some examples of salary ranges
        example_ranges = df_processed.select([
            'agency_name', 'title_description', 'salary_min', 'salary_max', 
            'salary_range_width', 'employee_count_in_role'
        ]).unique().head(10)
        
        print("\nExample salary ranges by agency and title:")
        print(example_ranges)
        
        return df_processed
    
    def create_fuzzy_matching_string(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create a fuzzy matching comparison string by combining Agency Name and Job Title"""
        
        print("Creating fuzzy matching strings...")
        
        df_processed = df.with_columns([
            # Clean and normalize agency name and title
            pl.col("agency_name").str.to_lowercase().str.strip_chars().alias("agency_clean"),
            pl.col("title_description").str.to_lowercase().str.strip_chars().alias("title_clean")
        ]).with_columns([
            # Create fuzzy matching string
            (pl.col("agency_clean") + "_" + pl.col("title_clean"))
            .str.replace_all(r'[^\w\s]', '', literal=False)  # Remove special characters
            .str.replace_all(r'\s+', '_', literal=False)     # Replace spaces with underscores
            .alias("fuzzy_match_string")
        ])
        
        print("Fuzzy matching strings created")
        return df_processed
    
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
        
        # Process the data in sequence
        df_processed = self.standardize_annual_salary(df_selected)
        df_processed = self.create_salary_min_max(df_processed)  # NEW: Added salary min/max creation
        df_processed = self.create_fuzzy_matching_string(df_processed)
        
        # Select final columns for output (UPDATED: Added new salary columns)
        final_columns = [
            'agency_name',
            'title_description',
            'base_salary',
            'pay_basis',
            'regular_hours', 
            'regular_gross_paid',
            'ot_hours',
            'total_ot_paid',
            'total_other_pay',
            'annual_salary_standardized',
            'salary_min',              # Min base salary for this agency-title combination
            'salary_max',              # Max base salary for this agency-title combination
            'salary_range_width',      # Difference between min and max
            'employee_count_in_role',  # NEW: Number of employees in this role
            'fuzzy_match_string'
        ]
        
        # Only select columns that exist
        final_columns = [col for col in final_columns if col in df_processed.columns]
        df_final = df_processed.select(final_columns)
        
        print(f"Processed DataFrame shape: {df_final.shape}")
        print(f"Final columns: {list(df_final.columns)}")
        
        return df_final
    
    def process_payroll_data(self, input_df: Optional[pl.DataFrame] = None, use_cache: bool = True) -> pl.DataFrame:
        """
        Main processing function for payroll data
        
        Args:
            input_df: Optional DataFrame to process (if None, will fetch fresh data)
            use_cache: Whether to use cached data for fetching
        """
        
        if input_df is None:
            print("No input DataFrame provided. Fetching fresh data...")
            fetcher = NYCPayrollDataFetcher()
            input_df = fetcher.fetch_data(max_rows=Config.DEVELOPMENT_ROW_LIMIT, use_cache=use_cache)
        
        if input_df.is_empty():
            print("No data to process")
            return pl.DataFrame()
        
        print("="*60)
        print("PROCESSING PAYROLL DATA")
        print("="*60)
        
        # Process the data
        processed_df = self.select_and_process_columns(input_df)
        
        # Save processed data
        timestamp = Config.get_timestamp()
        processed_file = self.save_processed_data(processed_df, f"nyc_payroll_processed_{timestamp}")
        
        # Create processing metadata (UPDATED: Added salary min/max transformation)
        processing_metadata = {
            'processing_type': 'payroll_standardization',
            'input_rows': input_df.shape[0],
            'output_rows': processed_df.shape[0],
            'columns_selected': list(processed_df.columns),
            'transformations': [
                'column_selection',
                'annual_salary_standardization',
                'salary_min_max_calculation',  # NEW
                'fuzzy_matching_string_creation'
            ],
            'processed_timestamp': Config.get_timestamp()
        }
        
        self.save_metadata(processing_metadata, f"payroll_processing_{timestamp}")
        
        print(f"\n✅ Processing complete!")
        print(f"Processed data saved to: {processed_file}")
        print(f"Sample of processed data:")
        print(processed_df.head())
        
        return processed_df

def main():
    """Main function to process payroll data"""
    processor = PayrollDataProcessor()
    
    print("=== PAYROLL DATA PROCESSING ===")
    processed_df = processor.process_payroll_data(use_cache=True)
    
    if not processed_df.is_empty():
        print(f"\nFinal processed dataset shape: {processed_df.shape}")
        print("\nSample salary standardization:")
        print(processed_df.select(['agency_name', 'title_description', 'pay_basis', 'base_salary', 'annual_salary_standardized']).head())
        
        print("\nSample salary ranges (grouped by agency and title):")
        print(processed_df.select(['agency_name', 'title_description', 'salary_min', 'salary_max', 'salary_range_width', 'employee_count_in_role']).head())
        
        # Show roles with the biggest salary ranges
        if 'salary_range_width' in processed_df.columns:
            print("\nTop 5 roles with largest salary ranges:")
            top_ranges = processed_df.select([
                'agency_name', 'title_description', 'salary_min', 'salary_max', 
                'salary_range_width', 'employee_count_in_role'
            ]).unique().sort('salary_range_width', descending=True).head(5)
            print(top_ranges)
        
        print("\nSample fuzzy matching strings:")
        print(processed_df.select(['agency_name', 'title_description', 'fuzzy_match_string']).head())

if __name__ == "__main__":
    main()