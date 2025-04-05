import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv

class SupabaseUploader:
    def __init__(self, supabase_url=None, supabase_key=None):
        """
        Initialize the Supabase uploader with credentials.
        
        Args:
            supabase_url: Supabase project URL
            supabase_key: Supabase project API key
        """
        # Try to load from environment if not provided
        load_dotenv()
        
        self.supabase_url = supabase_url or os.getenv("SUPABASE_URL")
        self.supabase_key = supabase_key or os.getenv("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase URL and key must be provided or set as environment variables")
        
        self.supabase = create_client(self.supabase_url, self.supabase_key)
    
    def map_fields(self, df, field_mapping=None):
        """
        Prepare DataFrame for upload to Supabase by selecting and renaming columns if needed.
        
        Args:
            df: pandas DataFrame to prepare
            field_mapping: Optional dictionary mapping original column names to desired column names
        
        Returns:
            DataFrame ready for upload
        """
        # Define the columns that match our database schema
        db_columns = [
            'docid', 'report_year', 
            'filer_first_name', 'filer_last_name',
            'member_first_name', 'member_last_name', 'member_full_name',
            'member_state', 'member_district', 'filingtype',
            'destination_city', 'destination_state',
            'departuredate', 'returndate', 
            'travel_sponsor', 'date_scraped'
        ]
        
        # Create a copy to avoid modifying the original
        mapped_df = df.copy()
        
        # Apply custom field mapping if provided
        if field_mapping:
            mapped_df = mapped_df.rename(columns=field_mapping)
        
        # Select only columns that exist in the DataFrame and match our schema
        columns_to_keep = [col for col in db_columns if col in mapped_df.columns]
        mapped_df = mapped_df[columns_to_keep]
        
        return mapped_df
    ##This function cleans up holes in the data where there are NaN values
    def handle_nan_values(self, df):
            """
            Clean the DataFrame by replacing NaN values with appropriate values based on column type.
            
            Args:
                df: pandas DataFrame to clean
                
            Returns:
                DataFrame with NaN values replaced appropriately
            """
            # Make a copy to avoid modifying the original
            clean_df = df.copy()
            
            # Automatically determine column types from data
            numeric_columns = []
            text_columns = []
            
            for col in clean_df.columns:
                # Check if column has integer or float data type
                if pd.api.types.is_numeric_dtype(clean_df[col]):
                    numeric_columns.append(col)
                else:
                    text_columns.append(col)
            
            print(f"Detected numeric columns: {numeric_columns}")
            print(f"Detected text columns: {text_columns}")
            
            # Replace NaN in text columns with 'badvalue'
            for col in text_columns:
                clean_df[col] = clean_df[col].fillna('badvalue')
            
            # For numeric columns, preserve the NaN as None for database NULL
            for col in numeric_columns:
                # If it's an integer column, convert to nullable integer
                if pd.api.types.is_integer_dtype(clean_df[col]):
                    clean_df[col] = clean_df[col].astype('Int64')  # Pandas nullable integer type
                # If it's a float column, leave as is (NaN will become None/null in JSON)
            
            return clean_df
    
    def upload_data(self, df, table_name="house_travel_reports", field_mapping=None, max_records=None):
        """
        Upload data to Supabase.
        
        Args:
            df: pandas DataFrame containing the data to upload
            table_name: Name of the Supabase table to upload to
            field_mapping: Optional dictionary mapping DataFrame columns to table columns
            max_records: Optional limit on number of records to upload (for testing)
        
        Returns:
            Result of the upload operation
        """
        # Make a copy to avoid modifying the original
        upload_df = df.copy()
        # Apply the field mapping if provided
        if field_mapping:
            for old_name, new_name in field_mapping.items():
                if old_name in upload_df.columns:
                    upload_df = upload_df.rename(columns={old_name: new_name})
                    print(f"Renamed column '{old_name}' to '{new_name}'")
        
        # Now prepare the DataFrame for upload after renaming
        if hasattr(self, 'map_fields'):
            upload_df = self.map_fields(upload_df)
        
        # Handle NaN values appropriately
        upload_df = self.handle_nan_values(upload_df)

        if upload_df.empty:
            print("No data to upload after field mapping")
            return None
        
        # Limit records if specified (for testing purposes)
        if max_records and max_records < len(upload_df):
            print(f"Limiting to {max_records} records for testing")
            upload_df = upload_df.head(max_records)
        
        # Print column info for debugging
        print(f"Columns being uploaded: {', '.join(upload_df.columns)}")
        print(f"Number of records to upload: {len(upload_df)}")
        
        # Print a sample record for debugging
        if len(upload_df) > 0:
            print("Sample record (first row):")
            sample_record = upload_df.iloc[0].to_dict()
            for key, value in sample_record.items():
                print(f"  {key}: {value} (type: {type(value).__name__})")
        
        # Convert DataFrame to list of dictionaries
        records = upload_df.to_dict(orient='records')
        
        try:
            # Since DocID is not unique, use insert instead of upsert
            print("Using insert operation (DocID is not unique)")
            result = self.supabase.table(table_name).insert(records).execute()
            print(f"Successfully inserted {len(records)} records")
            return result
            
        except Exception as e:
            print(f"Error uploading data to Supabase: {str(e)}")
            
            # Let's check the table schema to help diagnose the issue
            try:
                print("\nAttempting to inspect table schema...")
                # This is pseudo-code since Supabase doesn't have a direct schema inspection method
                # You might need to adjust based on your Supabase client capabilities
                schema_info = self.supabase.rpc('get_table_info', {'table_name': table_name}).execute()
                print(f"Schema info: {schema_info}")
            except Exception as schema_e:
                print(f"Could not retrieve schema: {schema_e}")
            
            # If the error is related to a specific record, try to identify it
            try:
                # Attempt to upload one record with minimal fields to diagnose the issue
                print("\nAttempting minimal record upload for diagnosis...")
                if len(records) > 0:
                    minimal_record = {
                        'report_year': records[0].get('report_year', '2025'),
                        'date_scraped': records[0].get('date_scraped', '2025-04-05')
                    }
                    test_result = self.supabase.table(table_name).insert([minimal_record]).execute()
                    print(f"Minimal record upload succeeded: {test_result}")
                
                # Now try one by one with all fields
                print("\nAttempting to identify problematic records...")
                for i, record in enumerate(records):
                    try:
                        self.supabase.table(table_name).insert([record]).execute()
                        print(f"Record {i} uploaded successfully")
                    except Exception as e2:
                        print(f"Error with record {i}: {str(e2)}")
                        print(f"Record content:")
                        for key, value in record.items():
                            print(f"  {key}: {value} (type: {type(value).__name__})")
            except Exception as e3:
                print(f"Error in error handler: {str(e3)}")
            
            return None
# Example usage
if __name__ == "__main__":
    # Load data from CSV (generated by the scraper)
    df = pd.read_csv("travel_reports.csv")
    
    # Initialize uploader and upload data
    uploader = SupabaseUploader()  # This will use credentials from .env file
    uploader.upload_data(df) ##TESTING added max_records = 10 - remove for real thing