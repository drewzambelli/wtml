import os
import time
import pandas as pd
import requests
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from supabase import create_client
from urllib.parse import urljoin
from dotenv import load_dotenv

# Supabase configuration
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Verify that credentials were loaded
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not found in .env file")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Directory for saving images locally
IMAGE_DIR = "member-headshots"
os.makedirs(IMAGE_DIR, exist_ok=True)

# Configure Selenium
chrome_options = Options()
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option("useAutomationExtension", False)
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")

# Function to add random delays
def random_delay():
    time.sleep(2 + (time.time() % 3))

# Function to lookup internal_unique_id from house_travel_reports table
def get_internal_unique_id(member_data):
    if member_data.get("name"):
        raw_name = member_data["name"].strip()
        
        # Convert "Last, First" to "First Last" format
        if "," in raw_name:
            parts = raw_name.split(",", 1)
            formatted_name = f"{parts[1].strip()} {parts[0].strip()}"
            
            # Try exact match with formatted name
            response = supabase.table("house_travel_reports") \
                .select("internal_unique_id") \
                .eq("member_full_name", formatted_name) \
                .execute()
            
            if response.data and len(response.data) > 0:
                return response.data[0]["internal_unique_id"]
        
        # Try with raw name as fallback
        response = supabase.table("house_travel_reports") \
            .select("internal_unique_id") \
            .eq("member_full_name", raw_name) \
            .execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]["internal_unique_id"]

# Function to handle headshot storage
def handle_headshot(base_url, headshot_src, member_id):
    try:
        headshot_url = urljoin(base_url, headshot_src)
        headshot_filename = f"{member_id}.jpg"
        
        # Download the image
        img_response = requests.get(headshot_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        })
        
        if img_response.status_code == 200:
            # Save locally
            local_path = os.path.join(IMAGE_DIR, headshot_filename)
            with open(local_path, "wb") as f:
                f.write(img_response.content)
            
            # Upload to Supabase Storage
            with open(local_path, "rb") as f:
                supabase.storage.from_('member-headshots').upload(
                    headshot_filename,
                    f,
                    {"content-type": "image/jpeg"}
                )
            
            # Get the public URL
            file_url = supabase.storage.from_('member-headshots').get_public_url(headshot_filename)
            return headshot_filename, file_url
        return "", ""
    except Exception as e:
        return "", ""

def scrape_member_profile(driver, profile_url, member_data):
    base_url = "https://clerk.house.gov"
    full_url = urljoin(base_url, profile_url)
    
    driver.get(full_url)
    random_delay()
    
    # Wait for the page to load
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "about_bio"))
        )
    except TimeoutException:
        return None
    
    # Extract member data
    result = {}
    
    # Get internal_unique_id
    internal_id = get_internal_unique_id(member_data)
    if not internal_id:
        return None
    result["internal_unique_id"] = internal_id
    
    # Member full name
    try:
        name_element = driver.find_element(By.CLASS_NAME, "library-h1")
        result["member_full_name"] = name_element.text.strip()
    except NoSuchElementException:
        result["member_full_name"] = member_data.get("name", "")
    
    # State and district from CSV
    result["member_state"] = member_data.get("state", "")
    result["member_district"] = member_data.get("district", "")
    
    # Hometown
    try:
        hometown_element = driver.find_element(By.XPATH, "//p[contains(text(), 'Hometown:')]")
        result["member_hometown"] = hometown_element.text.replace("Hometown:", "").strip()
    except NoSuchElementException:
        result["member_hometown"] = ""
    
    # Contact address
    try:
        address_element = driver.find_element(By.XPATH, "//span[contains(@aria-label, 'Rayburn') or contains(@aria-label, 'Longworth') or contains(@aria-label, 'Cannon')]")
        result["member_contact"] = address_element.text.strip()
    except NoSuchElementException:
        result["member_contact"] = ""
    
    # Phone number
    try:
        phone_element = driver.find_element(By.XPATH, "//span[contains(@aria-label, 'phone') or contains(@class, 'phone')]")
        phone_text = phone_element.text
        result["member_phone"] = phone_text.replace("Phone:", "").strip() if "Phone:" in phone_text else phone_text.strip()
    except NoSuchElementException:
        result["member_phone"] = ""
    
    # Website
    try:
        website_element = driver.find_element(By.XPATH, "//span[contains(@class, 'phone')]/a")
        result["member_website"] = website_element.get_attribute("href")
    except NoSuchElementException:
        result["member_website"] = ""
    
    # Email - usually not provided directly
    result["member_email"] = ""
    
    # Headshot
    try:
        headshot_element = driver.find_element(By.XPATH, "//figure[contains(@class, 'about_bio-img')]/img")
        headshot_src = headshot_element.get_attribute("src")
        
        # Extract member_id from the URL for consistent naming
        member_id = profile_url.split('/')[-1]
        
        # Handle headshot operations
        headshot_filename, headshot_url = handle_headshot(base_url, headshot_src, member_id)
        
        result["headshot_filename"] = headshot_filename
        result["headshot_url"] = headshot_url
    except NoSuchElementException:
        result["headshot_filename"] = ""
        result["headshot_url"] = ""
    
    # Click to expand committees section
    try:
        committees_button = driver.find_element(By.XPATH, "//a[@aria-controls='collapsible-2']")
        driver.execute_script("arguments[0].click();", committees_button)
        random_delay()  # Wait for animation
        
        # Extract committees
        committee_links = driver.find_elements(By.XPATH, "//a[contains(@class, 'library-committeePanel-subItems') and not(ancestor::ul)]")
        
        # Initialize committee fields
        for i in range(1, 5):
            result[f"c_{i}"] = ""
            result[f"c_{i}link"] = ""
        
        # Fill in committee data
        for i, committee in enumerate(committee_links[:4], 1):  # Limit to 4 committees
            result[f"c_{i}"] = committee.text.strip()
            result[f"c_{i}link"] = urljoin(base_url, committee.get_attribute("href"))
            
            # Get subcommittees
            try:
                xpath = f"//a[contains(@class, 'library-committeePanel-subItems') and text()='{committee.text.strip()}']/following-sibling::ul[1]"
                subcommittee_ul = driver.find_elements(By.XPATH, xpath)
                
                if subcommittee_ul:
                    subcommittee_links = subcommittee_ul[0].find_elements(By.TAG_NAME, "a")
                    
                    # Initialize subcommittee fields
                    for j in range(1, 5):
                        result[f"sc_{j}"] = ""
                        result[f"sc_{j}link"] = ""
                    
                    # Fill in subcommittee data
                    for j, subcommittee in enumerate(subcommittee_links[:4], 1):
                        result[f"sc_{j}"] = subcommittee.text.strip()
                        result[f"sc_{j}link"] = urljoin(base_url, subcommittee.get_attribute("href"))
            except Exception:
                pass
    except Exception:
        pass
    
    # Add today's date in ISO format for the date_scraped field
    result["date_scraped"] = datetime.date.today().isoformat()
    
    return result

def main():
    # Load member links from CSV
    df = pd.read_csv("member_links.csv")
    
    # # For testing, just use a few records - COMMENT OUT WHEN YOU ACTUALLY WANT TO PROCESS ALL RECORDS
    # test_records = 10 #change this number to number of recs you want to process for testing purposes
    # df = df.head(test_records)
    
    # Initialize WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        for _, row in df.iterrows():
            member_data = {
                "member_id": row.get("member_id", ""),
                "profile_url": row["profile_url"],
                "name": row.get("name", ""),
                "state": row.get("state", ""),
                "district": row.get("district", ""),
                "hometown": row.get("hometown", ""),
                "party": row.get("party", "")
            }
            
            # Step 1: Scrape the data
            result = scrape_member_profile(driver, member_data["profile_url"], member_data)
            if not result:
                continue
            
            # Step 2: Insert the data
            try:
                # Try upsert to handle both new and existing records
                supabase.table('member_details').upsert(result).execute()
            except Exception:
                pass
            
            # Be nice to the server
            time.sleep(5 + (time.time() % 5))
    finally:
        driver.quit()

if __name__ == "__main__":
    main()



















# import os
# import time
# import pandas as pd
# import requests
# import datetime
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException, NoSuchElementException
# from supabase import create_client
# from urllib.parse import urljoin
# from dotenv import load_dotenv

# print("Script starting...1")

# # Supabase configuration
# load_dotenv()
# # Supabase configuration from environment variables
# SUPABASE_URL = os.getenv("SUPABASE_URL")
# SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# print("Script starting...2")

# # Verify that credentials were loaded
# if not SUPABASE_URL or not SUPABASE_KEY:
#     raise ValueError("Supabase credentials not found in .env file")

# supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# print("Script starting...3")
# # Directory for saving images locally
# IMAGE_DIR = "member-headshots"
# os.makedirs(IMAGE_DIR, exist_ok=True)

# # Configure Selenium to behave more like a human
# chrome_options = Options()
# chrome_options.add_argument("--window-size=1920,1080")
# chrome_options.add_argument("--disable-gpu")
# chrome_options.add_argument("--disable-blink-features=AutomationControlled")
# chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
# chrome_options.add_experimental_option("useAutomationExtension", False)
# print("Script starting...4")
# # User agent that looks like a regular browser
# chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")

# # Function to add random delays to avoid detection
# def random_delay():
#     time.sleep(2 + (time.time() % 3))
# print("Script starting...5")

# # Function to lookup internal_unique_id from house_travel_reports table
# def get_internal_unique_id(member_data):
#     """
#     Look up the internal_unique_id from house_travel_reports table based on member information.
#     Focuses on matching by member_full_name as the primary method.
#     """
#     print("Script starting...6")
#     # Try to match by full name first (most reliable)
#     if member_data.get("name"):
#         # Get the raw name from CSV data
#         raw_name = member_data["name"].strip()
#         print("RAW NAME:", raw_name)
        
#         # Convert "Last, First" to "First Last" format for comparison
#         if "," in raw_name:
#             parts = raw_name.split(",", 1)
#             last_name = parts[0].strip()
#             first_name = parts[1].strip()
#             formatted_name = f"{first_name} {last_name}"
#             print("FORMATTED NAME:", formatted_name)
            
#             # Try exact match with formatted name
#             response = supabase.table("house_travel_reports") \
#                 .select("internal_unique_id") \
#                 .eq("member_full_name", formatted_name) \
#                 .execute()
            
#             if response.data and len(response.data) > 0:
#                 member_info = response.data[0]
#                 print(f"IN TRY, member name: {member_info}")
#                 return response.data[0]["internal_unique_id"]
        
#         # If we didn't find a match with the formatted name, try the original name
#         # (This is a fallback in case some names are already in the right format)
#         response = supabase.table("house_travel_reports") \
#             .select("internal_unique_id") \
#             .eq("member_full_name", raw_name) \
#             .execute()
        
#         if response.data and len(response.data) > 0:
#             member_info = response.data[0]
#             print(f"IN TRY with raw name, member name: {member_info}")
#             return response.data[0]["internal_unique_id"]
        
#         # Continue with the rest of your matching logic...
#         # (partial match, last name only, etc.)

# # New function to handle headshot storage
# def handle_headshot(base_url, headshot_src, member_id):
#     """Separate function to handle headshot download and storage to isolate errors"""
#     try:
#         headshot_url = urljoin(base_url, headshot_src)
#         headshot_filename = f"{member_id}.jpg"
        
#         # Download the image
#         img_response = requests.get(headshot_url, headers={
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
#         })
        
#         if img_response.status_code == 200:
#             # Save locally
#             local_path = os.path.join(IMAGE_DIR, headshot_filename)
#             with open(local_path, "wb") as f:
#                 f.write(img_response.content)
            
#             try:
#                 # Upload to Supabase Storage
#                 with open(local_path, "rb") as f:
#                     supabase.storage.from_('member-headshots').upload(
#                         headshot_filename,
#                         f,
#                         {"content-type": "image/jpeg"}
#                     )
                
#                 # Get the public URL
#                 file_url = supabase.storage.from_('member-headshots').get_public_url(headshot_filename)
#                 return headshot_filename, file_url
#             except Exception as e:
#                 print(f"Error uploading to Supabase storage: {str(e)}")
#                 # Return just the local filename if Supabase upload fails
#                 return headshot_filename, ""
#         return "", ""
#     except Exception as e:
#         print(f"Error handling headshot: {str(e)}")
#         return "", ""

# def scrape_member_profile(driver, profile_url, member_data):
#     print("Script starting...7")
#     base_url = "https://clerk.house.gov"
#     full_url = urljoin(base_url, profile_url)
    
#     print(f"Accessing {full_url}")
#     driver.get(full_url)
#     random_delay()
    
#     # Wait for the page to load
#     try:
#         WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.CLASS_NAME, "about_bio"))
#         )
#     except TimeoutException:
#         print(f"Timeout loading page: {full_url}")
#         return None
    
#     # Extract member data
#     result = {}
    
#     # Get internal_unique_id - THIS IS THE NEW CRITICAL ADDITION
#     internal_id = get_internal_unique_id(member_data)
#     if internal_id:
#         result["internal_unique_id"] = internal_id
#     else:
#         print(f"Skipping member {member_data.get('name', 'Unknown')} - could not find internal_unique_id")
#         return None
    
#     # Member full name
#     try:
#         name_element = driver.find_element(By.CLASS_NAME, "library-h1")
#         result["member_full_name"] = name_element.text.strip()
#         print('IN TRY, member name:', result)
#     except NoSuchElementException:
#         result["member_full_name"] = member_data.get("name", "")
#         print('IN EXCEPTION, member name:', result)
#     # State and district from CSV
#     result["member_state"] = member_data.get("state", "")
#     result["member_district"] = member_data.get("district", "")
    
#     # Hometown
#     try:
#         hometown_element = driver.find_element(By.XPATH, "//p[contains(text(), 'Hometown:')]")
#         result["member_hometown"] = hometown_element.text.replace("Hometown:", "").strip()
#     except NoSuchElementException:
#         result["member_hometown"] = ""
    
#     # Contact address
#     try:
#         address_element = driver.find_element(By.XPATH, "//span[contains(@aria-label, 'Rayburn') or contains(@aria-label, 'Longworth') or contains(@aria-label, 'Cannon')]")
#         result["member_contact"] = address_element.text.strip()
#     except NoSuchElementException:
#         result["member_contact"] = ""
    
#     # Phone number
#     try:
#         phone_element = driver.find_element(By.XPATH, "//span[contains(@aria-label, 'phone') or contains(@class, 'phone')]")
#         phone_text = phone_element.text
#         if "Phone:" in phone_text:
#             result["member_phone"] = phone_text.replace("Phone:", "").strip()
#         else:
#             result["member_phone"] = phone_text.strip()
#     except NoSuchElementException:
#         result["member_phone"] = ""
    
#     # Website
#     try:
#         website_element = driver.find_element(By.XPATH, "//span[contains(@class, 'phone')]/a")
#         result["member_website"] = website_element.get_attribute("href")
#     except NoSuchElementException:
#         result["member_website"] = ""
    
#     # Email - usually not provided directly
#     result["member_email"] = ""
    
#     # Headshot - Use the new headshot handling function
#     try:
#         headshot_element = driver.find_element(By.XPATH, "//figure[contains(@class, 'about_bio-img')]/img")
#         headshot_src = headshot_element.get_attribute("src")
        
#         # Extract member_id from the URL for consistent naming
#         member_id = profile_url.split('/')[-1]
        
#         # Handle headshot operations in a separate function to isolate errors
#         headshot_filename, headshot_url = handle_headshot(base_url, headshot_src, member_id)
        
#         result["headshot_filename"] = headshot_filename
#         result["headshot_url"] = headshot_url
#     except NoSuchElementException:
#         result["headshot_filename"] = ""
#         result["headshot_url"] = ""
    
#     # Remove district map fields from result
#     # We're completely removing district map functionality as requested
    
#     # Click to expand committees section
#     try:
#         committees_button = driver.find_element(By.XPATH, "//a[@aria-controls='collapsible-2']")
#         driver.execute_script("arguments[0].click();", committees_button)
#         random_delay()  # Wait for animation
        
#         # Extract committees
#         committee_links = driver.find_elements(By.XPATH, "//a[contains(@class, 'library-committeePanel-subItems') and not(ancestor::ul)]")
        
#         # Initialize committee fields
#         for i in range(1, 5):
#             result[f"c_{i}"] = ""
#             result[f"c_{i}link"] = ""
        
#         # Fill in committee data
#         for i, committee in enumerate(committee_links[:4], 1):  # Limit to 4 committees
#             result[f"c_{i}"] = committee.text.strip()
#             result[f"c_{i}link"] = urljoin(base_url, committee.get_attribute("href"))
            
#             # Get the parent element of this committee to find its subcommittees
#             try:
#                 # Find the subcommittee list (ul) that follows this committee link
#                 xpath = f"//a[contains(@class, 'library-committeePanel-subItems') and text()='{committee.text.strip()}']/following-sibling::ul[1]"
#                 subcommittee_ul = driver.find_elements(By.XPATH, xpath)
                
#                 if subcommittee_ul:
#                     subcommittee_links = subcommittee_ul[0].find_elements(By.TAG_NAME, "a")
                    
#                     # Initialize subcommittee fields for this committee
#                     for j in range(1, 5):
#                         result[f"sc_{j}"] = ""
#                         result[f"sc_{j}link"] = ""
                    
#                     # Fill in subcommittee data - adjust indices to match your DB structure
#                     for j, subcommittee in enumerate(subcommittee_links[:4], 1):
#                         result[f"sc_{j}"] = subcommittee.text.strip()
#                         result[f"sc_{j}link"] = urljoin(base_url, subcommittee.get_attribute("href"))
#             except Exception as e:
#                 print(f"Error getting subcommittees: {str(e)}")
#     except Exception as e:
#         print(f"Error accessing committees: {str(e)}")
    
#     # Add today's date in ISO format for the date_scraped field
#     result["date_scraped"] = datetime.date.today().isoformat()
    
#     print(f"Successfully scraped data for {result.get('member_full_name', 'Unknown member')}")
#     return result

# def debug_insert(result):
#     print("Script starting...8")
#     print("\nPAY ATTENTION - Fields in result:")
#     for key, value in result.items():
#         print(f"  {key}: {value}")
    
#     """Detailed debugging for Supabase insert operations"""
#     try:
#         # Print detailed info about what we're trying to insert
#         print("\n----- DEBUG: INSERT OPERATION -----")
#         print(f"Inserting data for: {result.get('member_full_name', 'Unknown')}")
#         print(f"internal_unique_id: {result.get('internal_unique_id')}")
#         print(f"date_scraped: {result.get('date_scraped')}")
        
#         # Try the insert with explicit error handling
#         response = supabase.table('member_details').insert(result).execute()
#         print("Insert successful!")
#         return True
#     except Exception as e:
#         error_msg = str(e)
#         print(f"\n----- ERROR DETAILS -----")
#         print(f"Error type: {type(e).__name__}")
#         print(f"Error message: {error_msg}")
        
#         # Handle specific RLS issues
#         if "violates row-level security policy" in error_msg:
#             print("\nThis is definitely an RLS policy issue in your Supabase database.")
#             print("You need to run the following SQL in Supabase SQL Editor:")
#             print("\nALTER TABLE member_details DISABLE ROW LEVEL SECURITY;")
#             print("-- or --")
#             print("CREATE POLICY \"Enable all operations for authenticated users only\" ON member_details")
#             print("    USING (auth.role() = 'authenticated');")
#         # Handle duplicate key issues
#         elif "duplicate key value" in error_msg:
#             print("\nThis record already exists in the database.")
#             print("If you want to update it, you should use an UPDATE query instead of INSERT.")
            
#             # Try to do an upsert instead
#             print("Attempting UPSERT operation instead...")
#             try:
#                 # Use upsert operation - this will update if the record exists
#                 response = supabase.table('member_details').upsert(result).execute()
#                 print("Upsert successful!")
#                 return True
#             except Exception as upsert_error:
#                 print(f"Upsert also failed: {str(upsert_error)}")
#         return False


# def main():
#     print("Script starting...9")
#     # Test Supabase RLS first
#     print("Testing Supabase permissions...")
#     try:
#         # Try to disable RLS directly through SQL 
#         # (this may fail if you don't have permission, which is fine)
#         try:
#             supabase.rpc('alter_table_disable_rls', {'table_name': 'member_details'}).execute()
#             print("Successfully disabled RLS on member_details table.")
#         except Exception as e:
#             print(f"Note: Could not disable RLS programmatically: {str(e)}")
#             print("This is normal if you don't have admin rights.")
        
#         # Try a minimal insert as a test
#         test_data = {
#             "internal_unique_id": 99999,  # Consider changing this value if duplicate key issue
#             "member_full_name": "TEST RECORD DELETE ME",
#             "date_scraped": datetime.date.today().isoformat()
#         }
        
#         print("Attempting test insert...")
#         test_result = debug_insert(test_data)
#         if not test_result:
#             # If test insert failed, output SQL to fix RLS in Supabase
#             print("\n=============================")
#             print("RLS POLICY ISSUE DETECTED")
#             print("=============================")
#             print("Run this in the Supabase SQL Editor:")
#             print("ALTER TABLE member_details DISABLE ROW LEVEL SECURITY;")
#             print("\nOR create more permissive policies:")
#             print("DROP POLICY IF EXISTS \"any_policy_name\" ON member_details;")
#             print("CREATE POLICY \"Allow all operations for authenticated users\" ON member_details")
#             print("    USING (auth.role() = 'authenticated')")
#             print("    WITH CHECK (auth.role() = 'authenticated');")
            
#             user_input = input("Continue with scraping? (y/n): ")
#             if user_input.lower() != 'y':
#                 print("Exiting script...")
#                 return
#     except Exception as e:
#         print(f"Supabase test failed: {str(e)}")
#         return

#     # Load member links from CSV
#     try:
#         df = pd.read_csv("member_links.csv")
#     except Exception as e:
#         print(f"Error loading CSV: {str(e)}")
#         return
    
#     # For testing, just use a few records
#     test_records = 2  # Change this number to test with more or fewer records
#     df = df.head(test_records)
    
#     # Initialize WebDriver
#     try:
#         driver = webdriver.Chrome(options=chrome_options)
#         driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
#         driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
#     except Exception as e:
#         print(f"Error initializing WebDriver: {str(e)}")
#         return
    
#     try:
#         for _, row in df.iterrows():
#             member_data = {
#                 "member_id": row.get("member_id", ""),
#                 "profile_url": row["profile_url"],
#                 "name": row.get("name", ""),
#                 "state": row.get("state", ""),
#                 "district": row.get("district", ""),
#                 "hometown": row.get("hometown", ""),
#                 "party": row.get("party", "")
#             }
            
#             # Step 1: Scrape the data
#             result = None
#             try:
#                 result = scrape_member_profile(driver, member_data["profile_url"], member_data)
#                 if not result:
#                     print(f"No data returned for {member_data.get('name', 'Unknown')}")
#                     continue
#             except Exception as e:
#                 print(f"Error scraping {member_data['profile_url']}: {str(e)}")
#                 continue
            
#             # Step 2: Insert the data
#             try:
#                 if debug_insert(result):
#                     print(f"Successfully saved data for {result.get('member_full_name', 'Unknown member')}")
#                 else:
#                     print(f"Failed to save data for {result.get('member_full_name', 'Unknown member')}")
#             except Exception as e:
#                 print(f"Error during database operation: {str(e)}")
            
#             # Be nice to the server
#             time.sleep(5 + (time.time() % 5))
        
#     finally:
#         print("Script starting...10")
#         driver.quit()

# if __name__ == "__main__":
#     main()