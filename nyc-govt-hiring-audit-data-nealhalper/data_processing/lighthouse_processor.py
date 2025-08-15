import polars as pl
import sys
import os
from typing import Optional
from pathlib import Path
from difflib import SequenceMatcher

# Add parent directory to path to import from root level
sys.path.append(str(Path(__file__).parent.parent))

from base_pipeline import BaseDataPipeline
from config import Config
from data_acquisition.lighthouse_data import LighthouseDataProcessor as LighthouseDataFetcher

class LighthouseProcessor(BaseDataPipeline):
    def __init__(self):
        super().__init__("lighthouse_processor")
        
        # Define column mappings for standardization
        self.column_mappings = {
            # Common variations that might exist in the raw data
            'job_title': 'lighthouse_job_title',
            'title': 'lighthouse_job_title', 
            'Job Title': 'lighthouse_job_title',
            'total_postings': 'lighthouse_total_postings',
            'postings': 'lighthouse_total_postings',
            'Total Postings': 'lighthouse_total_postings',
            'median_posting_duration': 'lighthouse_median_posting_duration',
            'posting_duration': 'lighthouse_median_posting_duration',
            'Median Posting Duration': 'lighthouse_median_posting_duration',
            'median_salary': 'lighthouse_median_salary',
            'salary': 'lighthouse_median_salary',
            'Median Salary': 'lighthouse_median_salary',
            'skills': 'lighthouse_top_skills',
            'top_skills': 'lighthouse_top_skills',
            'Top Skills': 'lighthouse_top_skills',
            'education': 'lighthouse_education_requirements',
            'education_requirements': 'lighthouse_education_requirements',
            'Education Requirements': 'lighthouse_education_requirements',
            'experience': 'lighthouse_experience_level',
            'experience_level': 'lighthouse_experience_level',
            'Experience Level': 'lighthouse_experience_level'
        }
    
    def clean_column_names(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and standardize column names"""
        
        print("Cleaning and standardizing column names...")
        print(f"Original columns: {list(df.columns)}")
        
        # Create a mapping for actual columns in the dataframe
        rename_mapping = {}
        
        for col in df.columns:
            # Clean the column name first (remove spaces, special characters, etc.)
            cleaned_col = col.strip().lower().replace(' ', '_').replace('-', '_')
            cleaned_col = ''.join(char for char in cleaned_col if char.isalnum() or char == '_')
            
            # Check if we have a specific mapping for this column
            if col in self.column_mappings:
                rename_mapping[col] = self.column_mappings[col]
            elif cleaned_col in self.column_mappings:
                rename_mapping[col] = self.column_mappings[cleaned_col]
            else:
                # If no specific mapping, apply lighthouse prefix to cleaned name
                if not cleaned_col.startswith('lighthouse_'):
                    rename_mapping[col] = f'lighthouse_{cleaned_col}'
                else:
                    rename_mapping[col] = cleaned_col
        
        print(f"Column mappings: {rename_mapping}")
        
        # Rename columns
        df_renamed = df.rename(rename_mapping)
        
        print(f"Standardized columns: {list(df_renamed.columns)}")
        return df_renamed
    
    def clean_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and standardize data types"""
        
        print("Cleaning data types...")
        
        # Create a copy for processing
        df_processed = df.clone()
        
        # Standard type conversions for common Lighthouse columns
        type_conversions = {}
        
        for col in df_processed.columns:
            if 'posting' in col.lower() and 'duration' not in col.lower():
                # Total postings should be integer
                type_conversions[col] = pl.Int32
            elif 'salary' in col.lower():
                # Salary should be float
                type_conversions[col] = pl.Float64
            elif 'duration' in col.lower():
                # Duration should be float (days)
                type_conversions[col] = pl.Float64
            elif col.lower().endswith('_date'):
                # Dates should be datetime
                type_conversions[col] = pl.Datetime
        
        # Apply type conversions safely
        for col, dtype in type_conversions.items():
            if col in df_processed.columns:
                try:
                    df_processed = df_processed.with_columns([
                        pl.col(col).cast(dtype, strict=False).alias(col)
                    ])
                    print(f"Converted {col} to {dtype}")
                except Exception as e:
                    print(f"Warning: Could not convert {col} to {dtype}: {e}")
        
        return df_processed
    
    def create_analysis_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create additional columns for analysis"""
        
        print("Creating analysis columns...")
        
        df_processed = df
        
        # Add posting intensity (postings per day) if we have the right columns
        if 'lighthouse_total_postings' in df.columns and 'lighthouse_median_posting_duration' in df.columns:
            df_processed = df_processed.with_columns([
                (pl.col('lighthouse_total_postings') / 
                 pl.col('lighthouse_median_posting_duration').fill_null(30))
                .alias('lighthouse_posting_intensity')
            ])
            print("Added posting intensity column")
        
        # Create salary range categories if salary data exists
        if 'lighthouse_median_salary' in df.columns:
            df_processed = df_processed.with_columns([
                pl.when(pl.col('lighthouse_median_salary') < 40000)
                .then(pl.lit('Low'))
                .when(pl.col('lighthouse_median_salary') < 70000)
                .then(pl.lit('Medium'))
                .when(pl.col('lighthouse_median_salary') < 100000)
                .then(pl.lit('High'))
                .otherwise(pl.lit('Very High'))
                .alias('lighthouse_salary_category')
            ])
            print("Added salary category column")
        
        # Create fuzzy matching string for job titles (similar to other processors)
        if 'lighthouse_job_title' in df.columns:
            df_processed = df_processed.with_columns([
                pl.col('lighthouse_job_title')
                .str.to_lowercase()
                .str.strip_chars()
                .str.replace_all(r'[^\w\s]', '', literal=False)
                .str.replace_all(r'\s+', '_', literal=False)
                .alias('lighthouse_fuzzy_match_string')
            ])
            print("Added fuzzy matching string for job titles")
        
        return df_processed
    
    def process_lighthouse_data(self, input_df: Optional[pl.DataFrame] = None, use_cache: bool = True) -> pl.DataFrame:
        """
        Main processing function for Lighthouse data
        
        Args:
            input_df: Optional DataFrame to process (if None, will load from raw data)
            use_cache: Whether to use cached data for loading
        """
        
        if input_df is None:
            print("No input DataFrame provided. Loading from raw data...")
            
            # Try to load from raw data directory
            raw_files = list(self.config.RAW_DATA_DIRECTORY.glob("lighthouse_*.xlsx")) + \
                       list(self.config.RAW_DATA_DIRECTORY.glob("lighthouse_*.json")) + \
                       list(self.config.RAW_DATA_DIRECTORY.glob("lighthouse_*.csv"))
            
            if not raw_files:
                print("No raw Lighthouse data files found.")
                return pl.DataFrame()
            
            # Load the most recent file
            latest_file = sorted(raw_files)[-1]
            print(f"Loading raw data from: {latest_file}")
            
            try:
                if latest_file.suffix == '.xlsx':
                    input_df = pl.read_excel(latest_file)
                elif latest_file.suffix == '.json':
                    input_df = pl.read_json(latest_file)
                elif latest_file.suffix == '.csv':
                    input_df = pl.read_csv(latest_file)
                else:
                    print(f"Unsupported file format: {latest_file.suffix}")
                    return pl.DataFrame()
                    
            except Exception as e:
                print(f"Error loading raw data: {e}")
                return pl.DataFrame()
        
        if input_df.is_empty():
            print("No data to process")
            return pl.DataFrame()
        
        print("="*60)
        print("PROCESSING LIGHTHOUSE DATA")
        print("="*60)
        print(f"Input data shape: {input_df.shape}")
        
        # Process the data through all steps
        df_processed = self.clean_column_names(input_df)
        df_processed = self.clean_data_types(df_processed)
        df_processed = self.create_analysis_columns(df_processed)
        
        print(f"Processed data shape: {df_processed.shape}")
        print(f"Final columns: {list(df_processed.columns)}")
        
        # Save processed data
        timestamp = Config.get_timestamp()
        processed_file = self.save_processed_data(df_processed, f"lighthouse_processed_{timestamp}")
        
        # Create processing metadata
        processing_metadata = {
            'processing_type': 'lighthouse_data_processing',
            'input_rows': input_df.shape[0],
            'output_rows': df_processed.shape[0],
            'columns_selected': list(df_processed.columns),
            'transformations': [
                'column_name_standardization',
                'data_type_cleaning', 
                'analysis_column_creation',
                'fuzzy_matching_string_creation'
            ],
            'processed_timestamp': Config.get_timestamp()
        }
        
        self.save_metadata(processing_metadata, f"lighthouse_processing_{timestamp}")
        
        print(f"\n✅ Processing complete!")
        print(f"Processed data saved to: {processed_file}")
        print(f"Sample of processed data:")
        print(df_processed.head())
        
        return df_processed

    def generate_posting_duration_dataset(self, jobs_processed: pl.DataFrame, lighthouse_processed: pl.DataFrame) -> pl.DataFrame:
        """
        Generate a dataset matching job postings with lighthouse data using fuzzy matching
        
        Args:
            jobs_processed: Processed job postings DataFrame
            lighthouse_processed: Processed lighthouse DataFrame
            
        Returns:
            Combined DataFrame with matched job postings and lighthouse data
        """
        
        print("="*60)
        print("GENERATING POSTING DURATION DATASET")
        print("="*60)
        print(f"Jobs processed shape: {jobs_processed.shape}")
        print(f"Lighthouse processed shape: {lighthouse_processed.shape}")
        
        # Verify required columns exist
        required_jobs_cols = ['business_title', 'posting_duration_days']
        required_lighthouse_cols = ['lighthouse_job_title', 'lighthouse_median_posting_duration']
        
        missing_jobs_cols = [col for col in required_jobs_cols if col not in jobs_processed.columns]
        missing_lighthouse_cols = [col for col in required_lighthouse_cols if col not in lighthouse_processed.columns]
        
        if missing_jobs_cols:
            print(f"Warning: Missing required job columns: {missing_jobs_cols}")
        if missing_lighthouse_cols:
            print(f"Warning: Missing required lighthouse columns: {missing_lighthouse_cols}")
        
        if missing_jobs_cols or missing_lighthouse_cols:
            print("Cannot proceed with fuzzy matching due to missing columns")
            return pl.DataFrame()
        
        # Create lookup dictionary from lighthouse data
        lighthouse_lookup = {}
        for row in lighthouse_processed.iter_rows(named=True):
            job_title = row.get('lighthouse_job_title')
            if job_title:
                # Clean title for fuzzy matching
                clean_title = str(job_title).lower().strip()
                clean_title = ''.join(char for char in clean_title if char.isalnum() or char.isspace())
                clean_title = '_'.join(clean_title.split())
                
                lighthouse_lookup[clean_title] = {
                    'original_title': job_title,
                    'median_duration': row.get('lighthouse_median_posting_duration'),
                    'total_postings': row.get('lighthouse_total_postings'),
                    'median_salary': row.get('lighthouse_median_salary'),
                    'top_skills': row.get('lighthouse_top_skills'),
                    'education_requirements': row.get('lighthouse_education_requirements'),
                    'experience_level': row.get('lighthouse_experience_level'),
                    'salary_category': row.get('lighthouse_salary_category'),
                    'posting_intensity': row.get('lighthouse_posting_intensity')
                }
        
        lighthouse_titles = list(lighthouse_lookup.keys())
        print(f"Created lookup for {len(lighthouse_titles)} lighthouse job titles")
        
        # Function to find best fuzzy match
        def find_best_lighthouse_match(business_title, match_threshold=0.7):
            if not business_title or business_title is None:
                return None, 0.0, {}
            
            # Clean business title for matching
            clean_business_title = str(business_title).lower().strip()
            clean_business_title = ''.join(char for char in clean_business_title if char.isalnum() or char.isspace())
            clean_business_title = '_'.join(clean_business_title.split())
            
            best_match = None
            best_score = 0.0
            best_data = {}
            
            for lighthouse_title in lighthouse_titles:
                if lighthouse_title:
                    score = SequenceMatcher(None, clean_business_title, lighthouse_title).ratio()
                    if score > best_score and score >= match_threshold:
                        best_score = score
                        best_match = lighthouse_title
                        best_data = lighthouse_lookup[lighthouse_title]
            
            return best_match, best_score, best_data
        
        # Apply fuzzy matching
        print("Performing fuzzy matching between job postings and lighthouse data...")
        
        matches_data = []
        for row in jobs_processed.iter_rows(named=True):
            business_title = row.get('business_title')
            best_match, match_score, lighthouse_data = find_best_lighthouse_match(business_title)
            
            # Combine job posting data with matched lighthouse data
            combined_row = dict(row)  # Start with all job posting data
            
            # Add lighthouse match information
            combined_row['lighthouse_match_title'] = lighthouse_data.get('original_title') if best_match else None
            combined_row['lighthouse_match_score'] = match_score if best_match else None
            combined_row['lighthouse_median_posting_duration'] = lighthouse_data.get('median_duration') if best_match else None
            combined_row['lighthouse_total_postings'] = lighthouse_data.get('total_postings') if best_match else None
            combined_row['lighthouse_median_salary'] = lighthouse_data.get('median_salary') if best_match else None
            combined_row['lighthouse_top_skills'] = lighthouse_data.get('top_skills') if best_match else None
            combined_row['lighthouse_education_requirements'] = lighthouse_data.get('education_requirements') if best_match else None
            combined_row['lighthouse_experience_level'] = lighthouse_data.get('experience_level') if best_match else None
            combined_row['lighthouse_salary_category'] = lighthouse_data.get('salary_category') if best_match else None
            combined_row['lighthouse_posting_intensity'] = lighthouse_data.get('posting_intensity') if best_match else None
            
            # Calculate duration comparison if both durations exist
            if combined_row['posting_duration_days'] is not None and combined_row['lighthouse_median_posting_duration'] is not None:
                actual_duration = combined_row['posting_duration_days']
                expected_duration = combined_row['lighthouse_median_posting_duration']
                combined_row['duration_variance'] = actual_duration - expected_duration
                combined_row['duration_variance_pct'] = ((actual_duration - expected_duration) / expected_duration * 100) if expected_duration > 0 else None
            else:
                combined_row['duration_variance'] = None
                combined_row['duration_variance_pct'] = None
            
            matches_data.append(combined_row)
        
        # Convert to Polars DataFrame
        combined_df = pl.DataFrame(matches_data)
        
        # Show matching statistics
        match_stats = combined_df.select([
            pl.col('lighthouse_match_score').count().alias('total_jobs'),
            pl.col('lighthouse_match_score').is_not_null().sum().alias('jobs_with_lighthouse_match'),
            pl.col('lighthouse_match_score').mean().alias('avg_match_score'),
            pl.col('lighthouse_match_score').median().alias('median_match_score'),
            (pl.col('lighthouse_match_score') >= 0.8).sum().alias('high_quality_matches_80_plus'),
            pl.col('duration_variance').mean().alias('avg_duration_variance_days'),
            pl.col('duration_variance_pct').mean().alias('avg_duration_variance_pct')
        ])
        
        print("\nFuzzy matching statistics:")
        print(match_stats)
        
        # Save the combined dataset
        timestamp = Config.get_timestamp()
        combined_file = self.save_processed_data(combined_df, f"jobs_lighthouse_matched_{timestamp}")
        
        # Create metadata for the combined dataset
        combined_metadata = {
            'processing_type': 'jobs_lighthouse_matching',
            'input_jobs_rows': jobs_processed.shape[0],
            'input_lighthouse_rows': lighthouse_processed.shape[0],
            'output_rows': combined_df.shape[0],
            'columns_selected': list(combined_df.columns),
            'transformations': [
                'fuzzy_matching_business_title_to_lighthouse_job_title',
                'duration_variance_calculation',
                'lighthouse_data_enrichment'
            ],
            'match_threshold': 0.7,
            'match_statistics': {
                'total_jobs': match_stats.select('total_jobs').item(),
                'jobs_with_match': match_stats.select('jobs_with_lighthouse_match').item(),
                'avg_match_score': match_stats.select('avg_match_score').item(),
                'high_quality_matches': match_stats.select('high_quality_matches_80_plus').item()
            },
            'processed_timestamp': Config.get_timestamp()
        }
        
        self.save_metadata(combined_metadata, f"jobs_lighthouse_matching_{timestamp}")
        
        print(f"\n✅ Posting duration dataset generation complete!")
        print(f"Combined dataset saved to: {combined_file}")
        print(f"Sample of combined data:")
        print(combined_df.select([
            'business_title', 'posting_duration_days', 
            'lighthouse_match_title', 'lighthouse_match_score',
            'lighthouse_median_posting_duration', 'duration_variance'
        ]).head())
        
        return combined_df

def main():
    """Main function to process Lighthouse data"""
    processor = LighthouseProcessor()
    
    print("=== LIGHTHOUSE DATA PROCESSING ===")
    processed_df = processor.process_lighthouse_data(use_cache=True)
    
    if not processed_df.is_empty():
        print(f"\nFinal processed dataset shape: {processed_df.shape}")
        
        # Show sample of key columns if they exist
        key_columns = ['lighthouse_job_title', 'lighthouse_total_postings', 
                      'lighthouse_median_posting_duration', 'lighthouse_median_salary']
        available_key_columns = [col for col in key_columns if col in processed_df.columns]
        
        if available_key_columns:
            print(f"\nSample of key Lighthouse data:")
            print(processed_df.select(available_key_columns).head())
        
        # Show analysis columns if created
        analysis_columns = ['lighthouse_posting_intensity', 'lighthouse_salary_category', 
                          'lighthouse_fuzzy_match_string']
        available_analysis_columns = [col for col in analysis_columns if col in processed_df.columns]
        
        if available_analysis_columns:
            print(f"\nSample of analysis columns:")
            print(processed_df.select(['lighthouse_job_title'] + available_analysis_columns).head())
        
        # Demonstrate fuzzy matching with job postings if available
        print("\n=== TESTING FUZZY MATCHING FUNCTIONALITY ===")
        
        # Try to load processed job postings data for demonstration
        from pathlib import Path
        processed_files = list(processor.config.PROCESSED_DATA_DIRECTORY.glob("nyc_jobs_posting_processed_*.parquet"))
        
        if processed_files:
            print("Found processed job postings data. Testing fuzzy matching...")
            try:
                # Load the most recent job postings file
                latest_jobs_file = sorted(processed_files)[-1]
                jobs_df = pl.read_parquet(latest_jobs_file)
                print(f"Loaded job postings: {jobs_df.shape}")
                
                # Generate the combined dataset
                combined_df = processor.generate_posting_duration_dataset(jobs_df, processed_df)
                
                if not combined_df.is_empty():
                    print(f"\nFuzzy matching completed successfully!")
                    print(f"Combined dataset shape: {combined_df.shape}")
                    
                    # Show some interesting results
                    high_quality_matches = combined_df.filter(pl.col("lighthouse_match_score") >= 0.8)
                    if not high_quality_matches.is_empty():
                        print(f"\nHigh-quality matches (>=80%): {high_quality_matches.shape[0]} jobs")
                        print("Sample high-quality matches:")
                        sample_cols = ['business_title', 'lighthouse_match_title', 'lighthouse_match_score', 
                                     'posting_duration_days', 'lighthouse_median_posting_duration', 'duration_variance']
                        available_sample_cols = [col for col in sample_cols if col in high_quality_matches.columns]
                        print(high_quality_matches.select(available_sample_cols).head())
                
            except Exception as e:
                print(f"Error during fuzzy matching demonstration: {e}")
        else:
            print("No processed job postings data found. Run the jobs posting processor first to enable fuzzy matching.")

if __name__ == "__main__":
    main()