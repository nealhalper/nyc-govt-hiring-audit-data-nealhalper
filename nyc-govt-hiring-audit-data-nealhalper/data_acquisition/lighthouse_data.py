from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import polars as pl
from dotenv import load_dotenv
import os
import xlrd
import json
import re
import pandas as pd
from base_pipeline import BaseDataPipeline
from config import Config

load_dotenv()
CREDENTIALS = os.getenv('LIGHTHOUSE_CREDENTIALS')

def create_filename(sheet_name: str) -> str:
    """Convert sheet name to a clean filename"""
    # Remove problematic characters and convert to lowercase
    clean_name = re.sub(r'[^\w\s-]', '', sheet_name.lower())
    # Replace spaces with underscores
    clean_name = re.sub(r'[-\s]+', '_', clean_name)
    return f"lighthouse_{clean_name}"

def inspect_worksheets(file_path):
    """Inspect all worksheets to find ones with actual data tables"""
    workbook = xlrd.open_workbook(file_path)
    worksheet_info = []
    
    for sheet_idx in range(workbook.nsheets):
        sheet = workbook.sheet_by_index(sheet_idx)
        sheet_name = sheet.name
        
        try:
            # Count non-empty rows to identify data sheets
            data_rows = 0
            for row_idx in range(min(50, sheet.nrows)):  # Check first 50 rows
                row = sheet.row_values(row_idx)
                if any(cell is not None and str(cell).strip() != "" for cell in row):
                    data_rows += 1
            
            worksheet_info.append({
                'name': sheet_name,
                'data_rows': data_rows,
                'max_column': sheet.ncols,
                'max_row': sheet.nrows
            })
            
        except Exception as e:
            print(f"Error inspecting sheet '{sheet_name}': {e}")
    
    return worksheet_info

def read_worksheet_data(file_path, sheet_name, max_rows=1000):
    """Read data from a specific worksheet with error handling"""
    workbook = xlrd.open_workbook(file_path)
    
    # Find the sheet by name
    sheet = None
    for sheet_idx in range(workbook.nsheets):
        if workbook.sheet_by_index(sheet_idx).name == sheet_name:
            sheet = workbook.sheet_by_index(sheet_idx)
            break
    
    if sheet is None:
        print(f"Sheet '{sheet_name}' not found")
        return None, None
    
    # Extract page title (usually in the first few rows)
    page_title = None
    table_start_row = 0
    
    # Look for the title in the first 10 rows
    for row_idx in range(min(10, sheet.nrows)):
        row = sheet.row_values(row_idx)
        
        # Check if this row has content that could be a title
        if any(cell is not None and str(cell).strip() != "" for cell in row):
            # If this looks like a title row (single cell with content, or first significant row)
            non_empty_cells = [cell for cell in row if cell is not None and str(cell).strip() != ""]
            
            # If it's likely a title (single cell or very few cells with content)
            if len(non_empty_cells) <= 2 and not page_title:
                page_title = str(non_empty_cells[0]).strip() if non_empty_cells else ""
                continue
            
            # If we find a row with multiple columns, this is likely the header row
            elif len(non_empty_cells) > 2:
                table_start_row = row_idx
                break
    
    if not page_title:
        page_title = f"Sheet: {sheet_name}"
    
    chunk_size = 100
    chunks = []
    current_chunk = []
    headers = None
    
    # Start reading from the identified table start
    for row_idx in range(table_start_row, min(max_rows + table_start_row + 1, sheet.nrows)):
        row = sheet.row_values(row_idx)
        
        if row_idx == table_start_row:
            # Use the first data row as headers
            headers = []
            for j, cell in enumerate(row):
                if cell is not None and str(cell).strip() != "":
                    headers.append(str(cell).strip())
                else:
                    headers.append(f"col_{j}")
            continue
            
        # Skip empty rows
        if not any(cell is not None and str(cell).strip() != "" for cell in row):
            continue
            
        # Clean row data and match header length
        cleaned_row = []
        for j in range(len(headers)):
            if j < len(row) and row[j] is not None:
                cleaned_row.append(str(row[j]).strip())
            else:
                cleaned_row.append("")
        
        current_chunk.append(cleaned_row)
        
        if len(current_chunk) >= chunk_size:
            try:
                chunk_df = pl.DataFrame(current_chunk, schema=headers, orient="row")
                chunks.append(chunk_df)
                current_chunk = []
                print(f"  Processed chunk {len(chunks)} from {sheet_name}")
            except Exception as chunk_error:
                print(f"  Chunk error in {sheet_name}: {chunk_error}")
                current_chunk = []
                continue

    # Handle remaining rows
    if current_chunk:
        try:
            chunk_df = pl.DataFrame(current_chunk, schema=headers, orient="row")
            chunks.append(chunk_df)
        except Exception as final_chunk_error:
            print(f"  Final chunk error in {sheet_name}: {final_chunk_error}")
    
    if chunks:
        return pl.concat(chunks), page_title
    else:
        return None, page_title

# NEW: Create the LighthouseDataProcessor class that inherits from BaseDataPipeline
class LighthouseDataProcessor(BaseDataPipeline):
    def __init__(self):
        super().__init__("lighthouse_data")
        self.credentials = os.getenv('LIGHTHOUSE_CREDENTIALS')
        
    def process_excel_file(self, use_cache: bool = True):
        """Process lighthouse excel file with caching and raw data storage"""
        
        # Check for cached raw Excel file first
        if use_cache:
            cached_file = self.find_recent_raw_file(max_age_hours=48)  # Excel files cache longer
            if cached_file:
                print("Using cached Excel data")
                # Use the cached file path for processing
                excel_file_path = cached_file.parent / f"temp_{cached_file.name}"
                # Copy cached file to temp location for processing
                import shutil
                shutil.copy2(cached_file, excel_file_path)
                return self._process_excel_from_file(str(excel_file_path))
        
        # Download new file from Google Drive
        try:
            creds = Credentials.from_service_account_file(self.credentials)
            service = build('drive', 'v3', credentials=creds)
            
            DOCUMENT_ID = os.getenv('LIGHTHOUSE_DATA')
            
            # Test if file is accessible
            file_info = service.files().get(fileId=DOCUMENT_ID).execute()
            print(f"File found: {file_info.get('name')}")
            
            # Download file
            request = service.files().get_media(fileId=DOCUMENT_ID)
            content = request.execute()
            
            # Save raw Excel file to raw data directory
            timestamp = Config.get_timestamp()
            raw_excel_path = self.config.RAW_DATA_DIRECTORY / f"lighthouse_excel_{timestamp}.xls"
            with open(raw_excel_path, 'wb') as f:
                f.write(content)
            
            print(f"Raw Excel file saved: {raw_excel_path}")
            
            # Also create temp file for processing
            temp_file = 'downloaded_file.xls'
            with open(temp_file, 'wb') as f:
                f.write(content)
                
            # Check file size
            file_size = os.path.getsize(temp_file)
            print(f"File size: {file_size / (1024*1024):.2f} MB")
            
            # Process the file
            result = self._process_excel_from_file(temp_file, file_info)
            
            # Clean up temp file
            os.remove(temp_file)
            
            return result
            
        except HttpError as e:
            print(f"HTTP Error {e.resp.status}: {e}")
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def _process_excel_from_file(self, file_path: str, file_info: dict = None):
        """Process Excel file from local path"""
        # First, inspect all worksheets
        print("\nInspecting worksheets...")
        worksheet_info = inspect_worksheets(file_path)
        
        for info in worksheet_info:
            print(f"Sheet: {info['name']} - Data rows: {info['data_rows']} - Dimensions: {info['max_row']}x{info['max_column']}")
        
        # Find worksheets that likely contain data tables (not cover pages)
        data_sheets = [info for info in worksheet_info if info['data_rows'] > 5 and info['max_column'] > 2]
        
        if not data_sheets:
            print("No data sheets found. Trying all sheets...")
            data_sheets = worksheet_info
        
        print(f"\nAttempting to read {len(data_sheets)} potential data sheets...")
        
        # Store metadata for all sheets
        metadata = {
            'source_file': file_info.get('name') if file_info else 'cached_file',
            'processed_date': Config.get_timestamp(),
            'sheets': {}
        }
        
        successful_sheets = []
        
        # Try to read each data sheet
        for sheet_info in data_sheets:
            sheet_name = sheet_info['name']
            print(f"\nTrying to read sheet: {sheet_name}")
        
            try:
                df, title = read_worksheet_data(file_path, sheet_name, max_rows=1000)
            
                if df is not None:
                    print(f"✓ Successfully read {sheet_name}")
                    print(f"  Page Title: {title}")
                    print(f"  DataFrame shape: {df.shape}")
                    print(f"  Columns: {list(df.columns)}")
                    
                    # Create clean filename
                    filename = create_filename(sheet_name)
                    
                    # Save using the base pipeline method (saves to processed directory)
                    processed_file_path = self.save_processed_data(df, filename)
                    print(f"  Saved to: {processed_file_path}")
                    
                    # Store metadata
                    metadata['sheets'][sheet_name] = {
                        'title': title,
                        'filename': f"{filename}.parquet",
                        'shape': df.shape,
                        'columns': list(df.columns),
                        'data_types': {col: str(dtype) for col, dtype in df.schema.items()},
                        'original_dimensions': f"{sheet_info['max_row']}x{sheet_info['max_column']}"
                    }
                    
                    successful_sheets.append(sheet_name)
                    print(f"  First few rows:")
                    print(df.head())
                    print("\n" + "="*50)
                else:
                    print(f"✗ No data found in {sheet_name}")
                    if title:
                        print(f"  But found title: {title}")
                    
            except Exception as sheet_error:
                print(f"✗ Failed to read {sheet_name}: {sheet_error}")
        
        # Save metadata using base pipeline method
        metadata_filename = f"lighthouse_processing_{Config.get_timestamp()}"
        metadata_file_path = self.save_metadata(metadata, metadata_filename)
        
        print(f"\n🎉 Processing complete!")
        print(f"Successfully processed {len(successful_sheets)} sheets")
        print(f"Processed files saved to: {self.config.PROCESSED_DATA_DIRECTORY}")
        print(f"Metadata saved to: {metadata_file_path}")
        
        return {
            'successful_sheets': successful_sheets,
            'metadata': metadata,
            'processed_files': len(successful_sheets)
        }

# UPDATED: Replace the old main() function with this new version
def main():
    """Main function using the new pipeline structure"""
    processor = LighthouseDataProcessor()
    result = processor.process_excel_file(use_cache=True)
    
    if result:
        print(f"\nSUMMARY: Processed {result['processed_files']} sheets successfully")
    else:
        print("Processing failed")

if __name__ == "__main__":
    main()