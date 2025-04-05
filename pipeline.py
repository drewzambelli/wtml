import argparse
from gift_scraper import TravelReportsScraper
from upload import SupabaseUploader

def run_pipeline(use_xml=True, years=None, upload=True):
    """
    Run the complete pipeline: scrape data and upload to Supabase.
    
    Args:
        use_xml: If True, use XML files for parsing, otherwise use TXT files
        years: Optional list of years to process (e.g., ["2018", "2019"])
        upload: If True, upload data to Supabase
    """
    # Initialize scraper
    print("Initializing travel reports scraper...")
    scraper = TravelReportsScraper()
    
    # Scrape travel report links
    print("Scraping travel report links...")
    links = scraper.scrape_travel_report_links()
    
    if not links:
        print("No travel report links found. Exiting.")
        return
    
    print(f"Found {len(links)} travel report links.")
    
    # Filter links by year if specified
    if years:
        links = [link for link in links if link['year'] in years]
        print(f"Filtered to {len(links)} links for specified years: {', '.join(years)}")
    
    # Download and extract data
    print(f"Downloading and processing travel reports using {'XML' if use_xml else 'TXT'} files...")
    scraper.download_and_extract_data(links, use_xml=use_xml)
    
    # Get processed data as DataFrame
    df = scraper.get_dataframe()
    
    # Handle date formatting
    df = scraper.convert_dates(df)
    
    print(f"Extracted {len(df)} records from travel reports.")
    
    # Save to CSV for backup/review
    csv_filename = "travel_reports.csv"
    scraper.save_to_csv(csv_filename)
    print(f"Saved data to {csv_filename} for reference.")
    
    # Upload to Supabase if requested
    if upload and not df.empty:
        try:
            print("Uploading data to Supabase...")
            uploader = SupabaseUploader()
            
            # No field mapping needed as we've already structured the data correctly
            result = uploader.upload_data(df)
            
            if result:
                print("Upload complete!")
            else:
                print("Upload completed with errors. Check the logs for details.")
                
        except Exception as e:
            print(f"Error during Supabase upload: {e}")
    elif df.empty:
        print("No data to upload.")
    else:
        print("Skipping Supabase upload as requested.")
    
    print("Pipeline execution complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape House Gift Travel Filings and upload to Supabase")
    
    parser.add_argument("--xml", action="store_true", default=True,
                        help="Use XML files for data extraction (default)")
    parser.add_argument("--txt", action="store_true", 
                        help="Use TXT files for data extraction")
    parser.add_argument("--years", nargs="+", 
                        help="Specific years to process (e.g., --years 2020 2021)")
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip uploading to Supabase")
    
    args = parser.parse_args()
    
    # Determine file format preference
    use_xml = True
    if args.txt:
        use_xml = False
    
    # Run the pipeline
    run_pipeline(
        use_xml=use_xml,
        years=args.years,
        upload=not args.no_upload
    )