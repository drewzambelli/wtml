import pandas as pd
from supabase import create_client
import os
from dotenv import load_dotenv
import datetime

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
        
        # Mappings for members and staff
        self.member_id_mapping = {}  # Maps member_full_name to internal_unique_id
        self.staff_id_mapping = {}   # Maps staff_full_name to unique_staff_id
        self.next_staff_num = 1      # For generating staff0001, staff0002, etc.
        
        # Get current year for filtering
        self.current_year = datetime.datetime.now().year
    
    def load_member_details(self):
        """
        Load member details from the member_details table.
        """
        try:
            print("Fetching member IDs from member_details table...")
            response = self.supabase.table("member_details") \
                .select("member_full_name, internal_unique_id") \
                .execute()
            
            # Process results and update our mapping
            if response.data:
                for record in response.data:
                    name = record.get('member_full_name')
                    id_val = record.get('internal_unique_id')
                    if name and id_val:
                        self.member_id_mapping[name] = id_val
                
                print(f"Loaded {len(self.member_id_mapping)} members from member_details")
            else:
                print("No members found in member_details table")
        
        except Exception as e:
            print(f"Error fetching member details: {str(e)}")
    
    def load_staff_details(self):
        """
        Load existing staff IDs from the member_staff table.
        """
        try:
            print("Fetching staff IDs from member_staff table...")
            response = self.supabase.table("member_staff") \
                .select("staff_full_name, unique_staff_id") \
                .execute()
            
            # Process results and update our mapping
            if response.data:
                for record in response.data:
                    name = record.get('staff_full_name')
                    id_val = record.get('unique_staff_id')
                    if name and id_val:
                        self.staff_id_mapping[name] = id_val
                        
                        # Extract the number from staff ID to determine next_staff_num
                        if id_val.startswith('staff'):
                            try:
                                num = int(id_val[5:])  # Extract digits after 'staff'
                                self.next_staff_num = max(self.next_staff_num, num + 1)
                            except ValueError:
                                pass  # Not a numeric staff ID format
                
                print(f"Loaded {len(self.staff_id_mapping)} staff from member_staff")
                print(f"Next staff ID will be: staff{self.next_staff_num:04d}")
            else:
                print("No staff found in member_staff table")
        
        except Exception as e:
            print(f"Error fetching staff details: {str(e)}")
    
    def process_records(self, df):
        """
        Process records according to the sophisticated logic:
        1. For current year records:
           - If member found in member_details, use that ID
           - If member not found, skip the record
           - If filer != member, add staff record
        2. For previous year records:
           - If member found in member_details, use that ID
           - If member not found, skip the record
           - If filer != member, add staff record
        
        Args:
            df: pandas DataFrame containing travel reports
            
        Returns:
            Tuple of (processed_df, records_to_skip)
        """
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Load member and staff details if not already loaded
        if not self.member_id_mapping:
            self.load_member_details()
        
        if not self.staff_id_mapping:
            self.load_staff_details()
        
        # Track which staff records we need to insert later
        staff_to_insert = []
        
        # Records to skip (member not found for non-current year)
        records_to_skip = []
        
        # Process each row
        for index, row in result_df.iterrows():
            member_name = row['member_full_name']
            report_year = str(row['report_year']).strip()
            filer_first = row.get('filer_first_name', '')
            filer_last = row.get('filer_last_name', '')
            
            # Skip if member name is invalid
            if pd.isna(member_name) or member_name == 'badvalue':
                records_to_skip.append(index)
                continue
            
            # Check if member exists in our mapping
            if member_name in self.member_id_mapping:
                member_id = self.member_id_mapping[member_name]
                result_df.at[index, 'internal_unique_id'] = member_id
                
                # Check if this is a staff filing
                if filer_first and filer_last and not pd.isna(filer_first) and not pd.isna(filer_last):
                    filer_full_name = f"{filer_first} {filer_last}".strip()
                    
                    # Only consider as staff if filer name is different from member name
                    if filer_full_name != member_name:
                        # If we haven't seen this staff member before, create a new ID
                        if filer_full_name not in self.staff_id_mapping:
                            staff_id = f"staff{self.next_staff_num:04d}"
                            self.staff_id_mapping[filer_full_name] = staff_id
                            self.next_staff_num += 1
                            
                            # Add to the list of staff to insert
                            staff_to_insert.append({
                                'unique_staff_id': staff_id,
                                'staff_first': filer_first,
                                'staff_last': filer_last,
                                'staff_full_name': filer_full_name,
                                'member_name': member_name,
                                'member_id': member_id,
                                'year': int(report_year) if report_year.isdigit() else None,
                                'date_created': datetime.date.today().isoformat()
                            })
            else:
                # Member not found
                if str(report_year) == str(self.current_year):
                    # For current year, set ID to 0 - we'll process these records
                    print(f"WARNING: Current year record with unknown member '{member_name}' - setting ID to 0")
                    result_df.at[index, 'internal_unique_id'] = 0
                    
                    # If there's a filer, add them as staff with no member association
                    if filer_first and filer_last and not pd.isna(filer_first) and not pd.isna(filer_last):
                        filer_full_name = f"{filer_first} {filer_last}".strip()
                        
                        if filer_full_name not in self.staff_id_mapping:
                            staff_id = f"staff{self.next_staff_num:04d}"
                            self.staff_id_mapping[filer_full_name] = staff_id
                            self.next_staff_num += 1
                            
                            # Add to the list of staff to insert (with no member association)
                            staff_to_insert.append({
                                'unique_staff_id': staff_id,
                                'staff_first': filer_first,
                                'staff_last': filer_last,
                                'staff_full_name': filer_full_name,
                                'member_name': None,
                                'member_id': None,
                                'year': int(report_year) if report_year.isdigit() else None,
                                'date_created': datetime.date.today().isoformat()
                            })
                else:
                    # For previous years, skip records where member is not found
                    print(f"Skipping record: Member '{member_name}' not found in member_details for year {report_year}")
                    records_to_skip.append(index)
        
        # Insert new staff records if needed
        if staff_to_insert:
            print(f"Inserting {len(staff_to_insert)} new staff records...")
            try:
                self.supabase.table("member_staff").insert(staff_to_insert).execute()
                print("Successfully inserted new staff records")
            except Exception as e:
                print(f"Error inserting staff records: {str(e)}")
                # Print a sample of what we tried to insert
                if staff_to_insert:
                    print("Sample staff record:")
                    print(staff_to_insert[0])
        
        # Remove records that should be skipped
        if records_to_skip:
            print(f"Skipping {len(records_to_skip)} records where member was not found")
            result_df = result_df.drop(records_to_skip)
            result_df = result_df.reset_index(drop=True)
        
        # Display mapping for debugging
        print(f"Total unique members mapped: {len(self.member_id_mapping)}")
        print(f"Total unique staff mapped: {len(self.staff_id_mapping)}")
        
        # Display a few examples of member mappings
        print("Sample of member ID mapping:")
        i = 0
        for name, id_val in self.member_id_mapping.items():
            print(f"  {name}: {id_val}")
            i += 1
            if i >= 5:  # Show just a few examples
                break
                
        # Display a few examples of staff mappings
        print("Sample of staff ID mapping:")
        i = 0
        for name, id_val in self.staff_id_mapping.items():
            print(f"  {name}: {id_val}")
            i += 1
            if i >= 5:  # Show just a few examples
                break
        
        return result_df
    
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
            'internal_unique_id',  # Added this column to the schema
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
            # Ensure internal_unique_id is never NULL
            if col == 'internal_unique_id':
                clean_df[col] = clean_df[col].fillna(0).astype('Int64')
            # If it's an integer column, convert to nullable integer
            elif pd.api.types.is_integer_dtype(clean_df[col]):
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
        
        # Process records with sophisticated logic
        upload_df = self.process_records(upload_df)
        
        # Now prepare the DataFrame for upload after renaming
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
                        'internal_unique_id': 0,  # Ensure this field is present
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
    
    # You can test with a smaller set first
    # result = uploader.upload_data(df, max_records=10)
    
    # For production, use the full dataset
    result = uploader.upload_data(df)







# import pandas as pd
# from supabase import create_client
# import os
# from dotenv import load_dotenv

# class SupabaseUploader:
#     def __init__(self, supabase_url=None, supabase_key=None):
#         """
#         Initialize the Supabase uploader with credentials.
        
#         Args:
#             supabase_url: Supabase project URL
#             supabase_key: Supabase project API key
#         """
#         # Try to load from environment if not provided
#         load_dotenv()
        
#         self.supabase_url = supabase_url or os.getenv("SUPABASE_URL")
#         self.supabase_key = supabase_key or os.getenv("SUPABASE_KEY")
        
#         if not self.supabase_url or not self.supabase_key:
#             raise ValueError("Supabase URL and key must be provided or set as environment variables")
        
#         self.supabase = create_client(self.supabase_url, self.supabase_key)
        
#         # Member ID mapping dictionary
#         self.member_id_mapping = {}
#         self.next_id = 1
    
#     def assign_internal_unique_ids(self, df):
#         """
#         Assign a unique ID to each unique member_full_name and add it to the DataFrame.
        
#         Args:
#             df: pandas DataFrame containing member_full_name column
            
#         Returns:
#             DataFrame with internal_unique_id column added
#         """
#         # Create a copy to avoid modifying the original
#         result_df = df.copy()
        
#         # Check if we need to initialize the mapping from existing data
#         if not self.member_id_mapping:
#             try:
#                 # Try to get existing member IDs from the database
#                 print("Fetching existing member IDs from database...")
#                 response = self.supabase.table("house_travel_reports") \
#                     .select("member_full_name, internal_unique_id") \
#                     .execute()
                
#                 # Process results and update our mapping
#                 if response.data:
#                     for record in response.data:
#                         name = record.get('member_full_name')
#                         id_val = record.get('internal_unique_id')
#                         if name and id_val and name not in self.member_id_mapping:
#                             self.member_id_mapping[name] = id_val
#                             # Update next_id to be greater than any existing ID
#                             self.next_id = max(self.next_id, id_val + 1)
                    
#                     print(f"Loaded {len(self.member_id_mapping)} existing member IDs")
#                     print(f"Next available ID: {self.next_id}")
            
#             except Exception as e:
#                 print(f"Error fetching existing member IDs: {str(e)}")
#                 print("Will create new IDs for all members")
        
#         # Now assign IDs to the current DataFrame
#         # Create a new column for the internal unique ID
#         def get_or_assign_id(name):
#             if pd.isna(name) or name == 'badvalue':
#                 # Handle null/missing names by assigning a special ID (e.g., 0)
#                 return 0
            
#             if name not in self.member_id_mapping:
#                 self.member_id_mapping[name] = self.next_id
#                 self.next_id += 1
            
#             return self.member_id_mapping[name]
        
#         # Apply the function to create the internal_unique_id column
#         result_df['internal_unique_id'] = result_df['member_full_name'].apply(get_or_assign_id)
        
#         # Display mapping for debugging
#         print(f"Total unique members: {len(self.member_id_mapping)}")
#         print("Sample of member ID mapping:")
#         i = 0
#         for name, id_val in self.member_id_mapping.items():
#             print(f"  {name}: {id_val}")
#             i += 1
#             if i >= 5:  # Show just a few examples
#                 break
        
#         return result_df
    
#     def map_fields(self, df, field_mapping=None):
#         """
#         Prepare DataFrame for upload to Supabase by selecting and renaming columns if needed.
        
#         Args:
#             df: pandas DataFrame to prepare
#             field_mapping: Optional dictionary mapping original column names to desired column names
        
#         Returns:
#             DataFrame ready for upload
#         """
#         # Define the columns that match our database schema
#         db_columns = [
#             'docid', 'report_year', 
#             'filer_first_name', 'filer_last_name',
#             'member_first_name', 'member_last_name', 'member_full_name',
#             'internal_unique_id',  # Added this column to the schema
#             'member_state', 'member_district', 'filingtype',
#             'destination_city', 'destination_state',
#             'departuredate', 'returndate', 
#             'travel_sponsor', 'date_scraped'
#         ]
        
#         # Create a copy to avoid modifying the original
#         mapped_df = df.copy()
        
#         # Apply custom field mapping if provided
#         if field_mapping:
#             mapped_df = mapped_df.rename(columns=field_mapping)
        
#         # Select only columns that exist in the DataFrame and match our schema
#         columns_to_keep = [col for col in db_columns if col in mapped_df.columns]
#         mapped_df = mapped_df[columns_to_keep]
        
#         return mapped_df
        
#     def handle_nan_values(self, df):
#         """
#         Clean the DataFrame by replacing NaN values with appropriate values based on column type.
        
#         Args:
#             df: pandas DataFrame to clean
            
#         Returns:
#             DataFrame with NaN values replaced appropriately
#         """
#         # Make a copy to avoid modifying the original
#         clean_df = df.copy()
        
#         # Automatically determine column types from data
#         numeric_columns = []
#         text_columns = []
        
#         for col in clean_df.columns:
#             # Check if column has integer or float data type
#             if pd.api.types.is_numeric_dtype(clean_df[col]):
#                 numeric_columns.append(col)
#             else:
#                 text_columns.append(col)
        
#         print(f"Detected numeric columns: {numeric_columns}")
#         print(f"Detected text columns: {text_columns}")
        
#         # Replace NaN in text columns with 'badvalue'
#         for col in text_columns:
#             clean_df[col] = clean_df[col].fillna('badvalue')
        
#         # For numeric columns, preserve the NaN as None for database NULL
#         for col in numeric_columns:
#             # Ensure internal_unique_id is never NULL
#             if col == 'internal_unique_id':
#                 clean_df[col] = clean_df[col].fillna(0).astype('Int64')
#             # If it's an integer column, convert to nullable integer
#             elif pd.api.types.is_integer_dtype(clean_df[col]):
#                 clean_df[col] = clean_df[col].astype('Int64')  # Pandas nullable integer type
#             # If it's a float column, leave as is (NaN will become None/null in JSON)
        
#         return clean_df
    
#     def upload_data(self, df, table_name="house_travel_reports", field_mapping=None, max_records=None):
#         """
#         Upload data to Supabase.
        
#         Args:
#             df: pandas DataFrame containing the data to upload
#             table_name: Name of the Supabase table to upload to
#             field_mapping: Optional dictionary mapping DataFrame columns to table columns
#             max_records: Optional limit on number of records to upload (for testing)
        
#         Returns:
#             Result of the upload operation
#         """
#         # Make a copy to avoid modifying the original
#         upload_df = df.copy()
        
#         # Apply the field mapping if provided
#         if field_mapping:
#             for old_name, new_name in field_mapping.items():
#                 if old_name in upload_df.columns:
#                     upload_df = upload_df.rename(columns={old_name: new_name})
#                     print(f"Renamed column '{old_name}' to '{new_name}'")
        
#         # Assign internal unique IDs
#         upload_df = self.assign_internal_unique_ids(upload_df)
        
#         # Now prepare the DataFrame for upload after renaming
#         upload_df = self.map_fields(upload_df)
        
#         # Handle NaN values appropriately
#         upload_df = self.handle_nan_values(upload_df)

#         if upload_df.empty:
#             print("No data to upload after field mapping")
#             return None
        
#         # Limit records if specified (for testing purposes)
#         if max_records and max_records < len(upload_df):
#             print(f"Limiting to {max_records} records for testing")
#             upload_df = upload_df.head(max_records)
        
#         # Print column info for debugging
#         print(f"Columns being uploaded: {', '.join(upload_df.columns)}")
#         print(f"Number of records to upload: {len(upload_df)}")
        
#         # Print a sample record for debugging
#         if len(upload_df) > 0:
#             print("Sample record (first row):")
#             sample_record = upload_df.iloc[0].to_dict()
#             for key, value in sample_record.items():
#                 print(f"  {key}: {value} (type: {type(value).__name__})")
        
#         # Convert DataFrame to list of dictionaries
#         records = upload_df.to_dict(orient='records')
        
#         try:
#             # Since DocID is not unique, use insert instead of upsert
#             print("Using insert operation (DocID is not unique)")
#             result = self.supabase.table(table_name).insert(records).execute()
#             print(f"Successfully inserted {len(records)} records")
#             return result
            
#         except Exception as e:
#             print(f"Error uploading data to Supabase: {str(e)}")
            
#             # Let's check the table schema to help diagnose the issue
#             try:
#                 print("\nAttempting to inspect table schema...")
#                 # This is pseudo-code since Supabase doesn't have a direct schema inspection method
#                 # You might need to adjust based on your Supabase client capabilities
#                 schema_info = self.supabase.rpc('get_table_info', {'table_name': table_name}).execute()
#                 print(f"Schema info: {schema_info}")
#             except Exception as schema_e:
#                 print(f"Could not retrieve schema: {schema_e}")
            
#             # If the error is related to a specific record, try to identify it
#             try:
#                 # Attempt to upload one record with minimal fields to diagnose the issue
#                 print("\nAttempting minimal record upload for diagnosis...")
#                 if len(records) > 0:
#                     minimal_record = {
#                         'report_year': records[0].get('report_year', '2025'),
#                         'internal_unique_id': 0,  # Ensure this field is present
#                         'date_scraped': records[0].get('date_scraped', '2025-04-05')
#                     }
#                     test_result = self.supabase.table(table_name).insert([minimal_record]).execute()
#                     print(f"Minimal record upload succeeded: {test_result}")
                
#                 # Now try one by one with all fields
#                 print("\nAttempting to identify problematic records...")
#                 for i, record in enumerate(records):
#                     try:
#                         self.supabase.table(table_name).insert([record]).execute()
#                         print(f"Record {i} uploaded successfully")
#                     except Exception as e2:
#                         print(f"Error with record {i}: {str(e2)}")
#                         print(f"Record content:")
#                         for key, value in record.items():
#                             print(f"  {key}: {value} (type: {type(value).__name__})")
#             except Exception as e3:
#                 print(f"Error in error handler: {str(e3)}")
            
#             return None

# # Example usage
# if __name__ == "__main__":
#     # Load data from CSV (generated by the scraper)
#     df = pd.read_csv("travel_reports.csv")
    
#     # Initialize uploader and upload data
#     uploader = SupabaseUploader()  # This will use credentials from .env file
    
#     # You can test with a smaller set first
#     # result = uploader.upload_data(df, max_records=10)
    
#     # For production, use the full dataset
#     result = uploader.upload_data(df)