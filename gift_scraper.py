import requests
from bs4 import BeautifulSoup
import os
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
import re
from datetime import datetime
from typing import List, Dict, Any

class TravelReportsScraper:
    def __init__(self, base_url="https://disclosures-clerk.house.gov"):
        self.base_url = base_url
        self.filings_url = f"{base_url}/GiftTravelFilings"
        self.data = []
    
    def scrape_travel_report_links(self) -> List[Dict[str, str]]:
        """
        Get travel report links directly using URL patterns instead of scraping HTML.
        This approach bypasses the need for HTML parsing and handles dynamically loaded content.
        
        Returns:
            List of dictionaries containing year and download URL for each zip file
        """
        try:
            print(f"Fetching travel report links using direct approach...")
            
            links = []
            ##Need to update this to stop message 403 from appearing
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://disclosures-clerk.house.gov/GiftTravelFilings',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # Based on your HTML snippet, we know the direct URL pattern:
            # /public_disc/gift-pdfs/YYYYTravel.zip
            print("Trying direct file paths for years 2018-2025")
            
            # First try the newest years that are most likely to exist
            for year in range(2025, 2017, -1): ## range(2025, 2017, -1) - starting at just 2025 for testing
                file_url = f"{self.base_url}/public_disc/gift-pdfs/{year}Travel.zip"
                
                try:
                    # Instead of downloading the whole file, just send a HEAD request to check if it exists
                    print(f"Checking if file exists: {file_url}")
                    head_response = requests.head(file_url, headers=headers, timeout=5)
                    
                    if head_response.status_code == 200:
                        print(f"✓ Verified file exists: {file_url}")
                        links.append({
                            'year': str(year),
                            'url': file_url
                        })
                    else:
                        print(f"✗ File does not exist or not accessible: {file_url} (Status: {head_response.status_code})")
                except requests.RequestException as e:
                    print(f"Error checking file {file_url}: {e}")
            
            # If we found links with HEAD requests, return them
            if links:
                print(f"Found {len(links)} travel report links via direct URL checks")
                return links
            
            # If HEAD requests failed, try GET requests as a fallback
            # Some servers don't properly handle HEAD requests
            print("HEAD requests unsuccessful, trying GET requests as fallback...")
            links = []
            
            for year in range(2025, 2017, -1): ## range(2025, 2017, -1) - starting at just 2025 for testing
                file_url = f"{self.base_url}/public_disc/gift-pdfs/{year}Travel.zip"
                
                try:
                    # Make a GET request but only download the first few bytes to verify the file exists
                    print(f"Checking with GET request: {file_url}")
                    get_response = requests.get(file_url, headers=headers, timeout=5, stream=True)
                    
                    if get_response.status_code == 200:
                        # Read just a tiny bit to confirm it's really a zip file
                        content_start = next(get_response.iter_content(chunk_size=100), None)
                        get_response.close()  # Close the connection
                        
                        if content_start and content_start.startswith(b'PK'):  # ZIP file signature
                            print(f"✓ Confirmed ZIP file exists: {file_url}")
                            links.append({
                                'year': str(year),
                                'url': file_url
                            })
                        else:
                            print(f"✗ File exists but is not a ZIP file: {file_url}")
                    else:
                        print(f"✗ File does not exist: {file_url} (Status: {get_response.status_code})")
                except requests.RequestException as e:
                    print(f"Error with GET request for {file_url}: {e}")
            
            # Try a fallback to find links in the HTML if direct URLs failed
            if not links:
                print("Direct URL approach failed, trying to extract from HTML as fallback...")
                
                # Get the page content
                response = requests.get(self.filings_url, headers=headers)
                response.raise_for_status()
                content = response.text
                
                # Use a simple regex to find travel report zip links
                href_pattern = r'href="([^"]*?(\d{4})Travel\.zip)"'
                matches = re.findall(href_pattern, content)
                
                for href, year in matches:
                    full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                    links.append({
                        'year': year,
                        'url': full_url
                    })
                
                if links:
                    print(f"Found {len(links)} travel report links with regex in HTML")
            
            return links
            
        except Exception as e:
            print(f"Error in scrape_travel_report_links: {e}")
            return []
    
    def download_and_extract_data(self, year_links: List[Dict[str, str]], use_xml=True):
        """
        Download and extract data from the zip files.
        
        Args:
            year_links: List of dictionaries containing year and URL for each zip file
            use_xml: If True, use XML files for data extraction, otherwise use TXT files
        """
        for link_info in year_links:
            year = link_info['year']
            url = link_info['url']
            
            try:
                print(f"Downloading travel report for {year} from {url}...")
                response = requests.get(url)
                response.raise_for_status()
                
                # Extract files from the zip
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                    file_list = zip_ref.namelist()
                    print(f"Files in zip: {file_list}")
                    
                    # Choose either XML or TXT files based on preference
                    target_extension = '.xml' if use_xml else '.txt'
                    target_files = [f for f in file_list if f.endswith(target_extension)]
                    
                    print(f"Found {len(target_files)} {target_extension} files")
                    
                    for file in target_files:
                        with zip_ref.open(file) as f:
                            if use_xml:
                                self._process_xml_file(f, year)
                            else:
                                self._process_txt_file(f, year)
            
            except requests.RequestException as e:
                print(f"Error downloading travel report for {year}: {e}")
            except zipfile.BadZipFile:
                print(f"Invalid zip file for {year}")
    
    def _process_xml_file(self, file, year):
        """
        Process an XML file and extract travel report data based on the XML structure.
        
        Args:
            file: File-like object containing XML data
            year: The year of the report
        """
        # if year != "2025":
        #     print('skipping the {year}')
        #     return
        try:
            # Print some debug information
            print(f"Processing XML file for year {year}")
            
            tree = ET.parse(file)
            root = tree.getroot()
            
            # Add date scraped field
            date_scraped = datetime.now().strftime("%Y-%m-%d")
            
            # Debug: print root tag and count of travel records
            print(f"XML root tag: {root.tag}")
            travel_records = root.findall('.//Travel')
            print(f"Found {len(travel_records)} travel records")
            
            # Process all GiftTravel/Travel records
            # Note: We're using the exact structure from the XML file
            records_found = 0
            for record in root.findall('.//Travel'):
                travel_data = {
                    'report_year': year,
                    'date_scraped': date_scraped
                }
                
                # Extract all available fields
                for elem in record:
                    if elem.text:
                        # Store the raw field value
                        field_name = elem.tag
                        field_value = elem.text.strip()
                        travel_data[field_name] = field_value
                
                # Process FilerName (FIRSTNAME LASTNAME format)
                if 'FilerName' in travel_data:
                    names = travel_data['FilerName'].split(' ', 1)
                    travel_data['filer_first_name'] = names[0] if names else ''
                    travel_data['filer_last_name'] = names[1] if len(names) > 1 else ''
                
                # Process MemberName (LASTNAME, FIRSTNAME format)
                if 'MemberName' in travel_data:
                    if ',' in travel_data['MemberName']:
                        last_name, first_name = travel_data['MemberName'].split(',', 1)
                        travel_data['member_last_name'] = last_name.strip()
                        travel_data['member_first_name'] = first_name.strip()
                        travel_data['member_full_name'] = f"{first_name.strip()} {last_name.strip()}"
                    else:
                        # Handle cases without a comma
                        travel_data['member_last_name'] = travel_data['MemberName']
                        travel_data['member_first_name'] = ''
                        travel_data['member_full_name'] = travel_data['MemberName']
                
                # Process Destination (CITY, STATE format)
                if 'Destination' in travel_data:
                    if ',' in travel_data['Destination']:
                        city, state = travel_data['Destination'].split(',', 1)
                        travel_data['destination_city'] = city.strip()
                        travel_data['destination_state'] = state.strip()
                    else:
                        # Handle cases without a comma - this means foreign travel - so we put country in destination_city and 'FX' in destination_state to indicate it is foreign
                        travel_data['destination_city'] = travel_data['Destination'] 
                        travel_data['destination_state'] = 'FX' ##using FX to indciate foreign travel
                
                # Map some fields to the expected database field names
                field_mappings = {
                    'DocID': 'docid',
                    'State': 'member_state',
                    'District': 'member_district',
                    'Year': 'report_year',  # This will override the year from the zip filename if present
                    'FilingType': 'filingtype',
                    'DepartureDate': 'departuredate', 
                    'ReturnDate': 'returndate',
                    'TravelSponsor': 'travel_sponsor'
                }
                #Checking to see if there is a value  in State and District - if there isn't, the person is administrative staff/gov bureacrat
                if 'member_state' not in travel_data or not travel_data.get('member_state'):
                    travel_data['member_state'] = 'ADMIN'
                    
                if 'member_district' not in travel_data or not travel_data.get('member_district'):
                    travel_data['member_district'] = 'ADMIN'
                for xml_field, db_field in field_mappings.items():
                    if xml_field in travel_data:
                        travel_data[db_field] = travel_data[xml_field]
                
                self.data.append(travel_data)
                records_found += 1
            
            print(f"Successfully extracted {records_found} records from XML file")
                
        except ET.ParseError as e:
            print(f"Error parsing XML file from {year}: {e}")
            # Try to print the first part of the file for debugging
            try:
                file.seek(0)
                content = file.read(1000).decode('utf-8', errors='replace')
                print(f"First 1000 chars of problematic file: {content}")
            except:
                pass
        except Exception as e:
            print(f"Unexpected error processing XML for {year}: {e}")
            import traceback
            traceback.print_exc()
    
    def _process_txt_file(self, file, year):
        """
        Process a TXT file and extract travel report data.
        
        Args:
            file: File-like object containing TXT data
            year: The year of the report
        """
        try:
            # This would need to be customized based on the actual TXT format
            content = file.read().decode('utf-8')
            
            # Parse TXT file according to its structure
            # This is a placeholder implementation
            print(f"TXT file processing not fully implemented for {year}")
                
        except UnicodeDecodeError:
            print(f"Error decoding TXT file from {year}")
    
    def get_dataframe(self):
        """
        Convert extracted data to a pandas DataFrame.
        
        Returns:
            pandas.DataFrame: DataFrame containing all extracted travel report data
        """
        return pd.DataFrame(self.data)
    
    def save_to_csv(self, filename="travel_reports.csv"):
        """
        Save extracted data to a CSV file.
        
        Args:
            filename: Name of the output CSV file
        """
        df = self.get_dataframe()
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    
    def convert_dates(self, df):
        """
        Convert date strings to proper date format.
        
        Args:
            df: DataFrame containing date columns
        
        Returns:
            DataFrame with properly formatted date columns
        """
        # Make a copy to avoid modifying the original
        result_df = df.copy()
        
        # Try to convert date fields
        date_fields = ['departuredate', 'returndate']
        for field in date_fields:
            if field in result_df.columns:
                try:
                    # Handle different date formats
                    result_df[field] = pd.to_datetime(result_df[field], errors='coerce')
                    # Format as YYYY-MM-DD for database
                    result_df[field] = result_df[field].dt.strftime('%Y-%m-%d')
                except Exception as e:
                    print(f"Warning: Could not convert {field} to date format: {e}")
        
        return result_df

# Example usage
if __name__ == "__main__":
    scraper = TravelReportsScraper()
    links = scraper.scrape_travel_report_links()
    
    if links:
        print(f"Found {len(links)} travel report links:")
        for link in links:
            print(f"  {link['year']}: {link['url']}")
        
        # Default to XML files for data extraction
        scraper.download_and_extract_data(links, use_xml=True)
        
        # Convert to DataFrame and handle date formatting
        df = scraper.get_dataframe()
        df = scraper.convert_dates(df)
        
        print(f"Extracted {len(df)} records")
        scraper.save_to_csv()
    else:
        print("No travel report links found.")