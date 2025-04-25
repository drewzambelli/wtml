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

# Global counter for generating sequential unique IDs
current_id = 1

# Function to add random delays
def random_delay():
    time.sleep(2 + (time.time() % 3))

# Function to generate a unique internal ID for each member
def generate_internal_unique_id():
    global current_id
    unique_id = current_id
    current_id += 1
    return unique_id

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
    
    # Generate internal_unique_id
    result["internal_unique_id"] = generate_internal_unique_id()
    
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
    
    # Initialize committee and subcommittee fields
    for i in range(1, 6):
        result[f"c_{i}"] = ""
        result[f"c_{i}link"] = ""
    
    for i in range(1, 7):  # Initialize sc_1 through sc_6
        result[f"sc_{i}"] = ""
        result[f"sc_{i}link"] = ""
    
    # Click to expand committees section
    try:
        committees_button = driver.find_element(By.XPATH, "//a[@aria-controls='collapsible-2']")
        driver.execute_script("arguments[0].click();", committees_button)
        random_delay()  # Wait for animation
        
        # Extract committees
        committee_links = driver.find_elements(By.XPATH, "//a[contains(@class, 'library-committeePanel-subItems') and not(ancestor::ul)]")
        
        # Use a counter for all subcommittees
        sc_count = 1
        
        # Fill in committee data - up to 5 committees
        for i, committee in enumerate(committee_links[:5], 1):
            result[f"c_{i}"] = committee.text.strip()
            result[f"c_{i}link"] = urljoin(base_url, committee.get_attribute("href"))
            
            # Get subcommittees
            try:
                xpath = f"//a[contains(@class, 'library-committeePanel-subItems') and text()='{committee.text.strip()}']/following-sibling::ul[1]"
                subcommittee_ul = driver.find_elements(By.XPATH, xpath)
                
                if subcommittee_ul:
                    subcommittee_links = subcommittee_ul[0].find_elements(By.TAG_NAME, "a")
                    
                    # Fill in subcommittee data - up to 6 subcommittees total
                    for subcommittee in subcommittee_links:
                        # Stop if we've reached the maximum number of subcommittees
                        if sc_count > 6:
                            break
                            
                        result[f"sc_{sc_count}"] = subcommittee.text.strip()
                        result[f"sc_{sc_count}link"] = urljoin(base_url, subcommittee.get_attribute("href"))
                        sc_count += 1
            except Exception:
                pass
    except Exception:
        pass
    
    # Add district map fields (blank placeholders)
    result["district_map_filename"] = ""
    result["district_map_url"] = ""
    
    # Add today's date in ISO format for the date_scraped field
    result["date_scraped"] = datetime.date.today().isoformat()
    
    return result

def main():
    global current_id
    
    # Load member links from CSV
    df = pd.read_csv("member_links.csv")
    
    # # For testing, just use a few records - COMMENT OUT WHEN YOU ACTUALLY WANT TO PROCESS ALL RECORDS
    #test_records = 5 #change this number to number of recs you want to process for testing purposes
    #df = df.head(test_records)
    
    # Initialize WebDriver
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"})
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Check for the highest existing ID in the database to start incrementing from there
    try:
        response = supabase.table('member_details').select('internal_unique_id').order('internal_unique_id.desc').limit(1).execute()
        if response.data and len(response.data) > 0:
            # Start from the highest existing ID + 1
            current_id = response.data[0]['internal_unique_id'] + 1
        else:
            # No existing records, start from 1
            current_id = 1
    except Exception:
        # If there's an error getting the max ID, start from 1
        current_id = 1
    
    print(f"Starting with ID: {current_id}")
    
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
                print(f"Successfully inserted data for {member_data.get('name')} with ID: {result['internal_unique_id']}")
            except Exception as e:
                print(f"Error inserting data for {member_data.get('name')}: {str(e)}")
            
            # Be nice to the server
            time.sleep(5 + (time.time() % 5))
    finally:
        driver.quit()

if __name__ == "__main__":
    main()