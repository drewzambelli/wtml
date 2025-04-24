import os
import re
import time
import pandas as pd
import unicodedata
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime

##################################################
########This file is only to be run to extract the links to the members' profile pages
########Did it this way because it got too complicated extract the links and then visiting
########the links to extract the data.
########Saves links to member_links.csv - use to visit each profile page to build your members_details table

def normalize_text(text):
    """
    Normalize text by replacing accented characters with their ASCII equivalents
    and removing any other non-ASCII characters.
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text
    """
    if not text:
        return text
    
    # Mapping of common special characters to their ASCII equivalents
    special_chars_map = {
        'Á': 'A', 'á': 'a',
        'É': 'E', 'é': 'e',
        'Í': 'I', 'í': 'i',
        'Ó': 'O', 'ó': 'o',
        'Ú': 'U', 'ú': 'u',
        'Ü': 'U', 'ü': 'u',
        'Ñ': 'N', 'ñ': 'n',
        'Ç': 'C', 'ç': 'c',
        'Ã': 'a',  # Specific to your example
    }
    
    # First try mapping known special characters
    for special_char, replacement in special_chars_map.items():
        text = text.replace(special_char, replacement)
    
    # Then normalize and replace any remaining accented characters
    text = unicodedata.normalize('NFKD', text)
    text = ''.join([c for c in text if not unicodedata.combining(c)])
    
    # Replace any remaining non-ASCII characters with their closest ASCII equivalent
    text = text.encode('ascii', 'replace').decode('ascii')
    text = text.replace('?', '')  # Remove question marks from failed replacements
    
    return text.strip()

class MemberProfileLinksScraper:
    def __init__(self, base_url="https://clerk.house.gov"):
        """Initialize the scraper with the base URL"""
        self.base_url = base_url
        self.members_url = f"{self.base_url}/Members"
        
        # Data storage
        self.all_members = []
        
        # Setup Selenium
        self.options = Options()
        self.options.add_argument("--headless")  # Run in headless mode
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--window-size=1920,1080")
        
        # Add user-agent to make it look like a real browser
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Initialize the driver
        self.driver = webdriver.Chrome(options=self.options)
    
    def _wait_for_element(self, by, value, timeout=10):
        """Wait for an element to be present and return it"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            print(f"Timed out waiting for element: {value}")
            return None
    
    def _extract_member_info(self, member_element):
        """Extract member information from a member element"""
        try:
            # Extract link and ID
            link_element = member_element.find_element(By.CSS_SELECTOR, "a.library-link.members-link")
            profile_url = link_element.get_attribute('href')
            member_id = profile_url.split('/')[-1]
            
            # Extract name
            name_element = member_element.find_element(By.CSS_SELECTOR, "h2.member-name text")
            raw_name = name_element.text.strip()
            
            # Normalize name to fix encoding issues
            name = normalize_text(raw_name)
            
            # Extract other details
            try:
                state_raw = member_element.find_element(By.CSS_SELECTOR, ".state").text.strip()
                state = normalize_text(state_raw)
            except NoSuchElementException:
                state = ""
                
            try:
                district_raw = member_element.find_element(By.CSS_SELECTOR, ".district").text.strip()
                district = normalize_text(district_raw)
            except NoSuchElementException:
                district = ""
                
            try:
                hometown_raw = member_element.find_element(By.CSS_SELECTOR, ".hometown").text.strip()
                hometown = normalize_text(hometown_raw)
            except NoSuchElementException:
                hometown = ""
                
            try:
                party_raw = member_element.find_element(By.CSS_SELECTOR, ".party").text.strip()
                party = normalize_text(party_raw)
            except NoSuchElementException:
                party = ""
            
            return {
                'member_id': member_id,
                'profile_url': profile_url,
                'name': name,
                'raw_name': raw_name,  # Keep the original name for reference
                'state': state,
                'district': district,
                'hometown': hometown,
                'party': party,
                'date_scraped': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            print(f"Error extracting member info: {str(e)}")
            return None
    
    def _get_total_pages(self):
        """Get the total number of pages from pagination"""
        try:
            # Find pagination element
            pagination = self.driver.find_element(By.CSS_SELECTOR, "ul.bottompagination")
            
            # Get all page links
            page_links = pagination.find_elements(By.CSS_SELECTOR, "a.page")
            
            # Find the highest page number
            highest_page = 1
            for link in page_links:
                try:
                    page_num = int(link.text.strip())
                    highest_page = max(highest_page, page_num)
                except ValueError:
                    # Skip links with non-numeric text (e.g., "...")
                    pass
            
            return highest_page
        
        except Exception as e:
            print(f"Error getting total pages: {str(e)}")
            
            # Try to get the pagination info from text
            try:
                info_element = self.driver.find_element(By.CSS_SELECTOR, ".pagination_info")
                info_text = info_element.text
                match = re.search(r'of\s+(\d+)', info_text)
                if match:
                    total_items = int(match.group(1))
                    # Assuming 20 items per page
                    return (total_items + 19) // 20
            except:
                pass
            
            return 1
    
    def scrape_all_pages(self, max_pages=None):
        """Scrape all pages of member profiles"""
        try:
            # Navigate to the members page
            self.driver.get(self.members_url)
            
            # Wait for the members list to load
            members_list = self._wait_for_element(By.ID, "members")
            
            if not members_list:
                print("Failed to find members list. Page might not have loaded correctly.")
                # Save the page source for debugging
                with open("debug_page.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                print("Saved page source to debug_page.html")
                return []
            
            # Get total pages
            total_pages = self._get_total_pages()
            print(f"Total pages detected: {total_pages}")
            
            # Apply max_pages limit if specified
            if max_pages and max_pages < total_pages:
                total_pages = max_pages
                print(f"Limiting to first {max_pages} pages")
            
            # Process each page
            for page_num in range(1, total_pages + 1):
                print(f"Processing page {page_num} of {total_pages}")
                
                if page_num > 1:
                    # Click on the page number link
                    try:
                        # Find the page link
                        page_link = self.driver.find_element(By.XPATH, f"//ul[contains(@class, 'bottompagination')]/li/a[text()='{page_num}']")
                        page_link.click()
                        
                        # Wait for the page to load
                        time.sleep(3)  # Give it some time to load
                        
                        # Wait for the members list to update
                        members_list = self._wait_for_element(By.ID, "members")
                        
                    except Exception as e:
                        print(f"Error navigating to page {page_num}: {str(e)}")
                        
                        # Try direct URL navigation as fallback
                        try:
                            page_url = f"{self.members_url}#MemberProfiles?currentPage={page_num}"
                            self.driver.get(page_url)
                            time.sleep(3)  # Wait for page to load
                            members_list = self._wait_for_element(By.ID, "members")
                        except Exception as e2:
                            print(f"Failed to navigate to page {page_num} via URL: {str(e2)}")
                            continue
                
                # Extract members from current page
                try:
                    member_elements = self.driver.find_elements(By.CSS_SELECTOR, "#members > li")
                    print(f"Found {len(member_elements)} member elements on page {page_num}")
                    
                    for member_element in member_elements:
                        member_info = self._extract_member_info(member_element)
                        if member_info:
                            self.all_members.append(member_info)
                    
                    print(f"Extracted {len(member_elements)} members from page {page_num}")
                
                except Exception as e:
                    print(f"Error extracting members from page {page_num}: {str(e)}")
                    # Save the page source for debugging
                    with open(f"debug_page_{page_num}.html", "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                    print(f"Saved page source to debug_page_{page_num}.html")
            
            print(f"Total members extracted: {len(self.all_members)}")
            return self.all_members
        
        finally:
            # Close the browser
            self.driver.quit()
    
    def save_to_csv(self, filename="member_links.csv"):
        """Save the scraped member links to a CSV file"""
        if not self.all_members:
            print("No member data to save")
            return None
        
        # Convert to DataFrame and save
        df = pd.DataFrame(self.all_members)
        
        # Check for character encoding issues
        for index, row in df.iterrows():
            if 'name' in row and row['name'] != normalize_text(row['name']):
                print(f"Warning: Character encoding issue detected in name: {row['name']}")
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')  # Using utf-8-sig for Excel compatibility
        print(f"Saved {len(self.all_members)} member links to {filename}")
        return df

def run_scraper(max_pages=None):
    """Run the scraper and save results to CSV"""
    scraper = MemberProfileLinksScraper()
    scraper.scrape_all_pages(max_pages)
    return scraper.save_to_csv()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape House member profile links")
    parser.add_argument("--max-pages", type=int, default=None, 
                        help="Maximum number of pages to scrape")
    
    args = parser.parse_args()
    
    df = run_scraper(max_pages=args.max_pages)