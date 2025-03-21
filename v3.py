#!/usr/bin/env python3
import os
import csv
import time
import random
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StartupRegistryCrawler:
    def __init__(self, base_url, output_file, headless=True, delay_range=(1, 3)):
        self.base_url = base_url
        self.output_file = output_file
        self.delay_range = delay_range
        self.setup_driver(headless)
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)

    def setup_driver(self, headless):
        """Set up the Selenium WebDriver with appropriate options."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Add a user agent to appear more like a normal browser
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info("WebDriver set up successfully")
        except Exception as e:
            logger.error(f"Failed to set up WebDriver: {e}")
            raise

    def random_delay(self):
        """Implement random delay between requests to avoid detection."""
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)

    def get_page(self, url):
        """Load a page using Selenium and wait for it to be fully rendered."""
        try:
            self.random_delay()
            self.driver.get(url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Scroll down to load any lazy-loaded content
            self.scroll_page()
            
            return self.driver.page_source
        except TimeoutException:
            logger.error(f"Timeout while loading {url}")
            return None
        except Exception as e:
            logger.error(f"Error loading {url}: {e}")
            return None

    def scroll_page(self):
        """Scroll down the page to load lazy-loaded content."""
        try:
            # Get scroll height
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while True:
                # Scroll down to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait to load page
                time.sleep(1)
                
                # Calculate new scroll height and compare with last scroll height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception as e:
            logger.warning(f"Error during page scrolling: {e}")

    def find_company_links(self):
        """Find all company links on the current page using multiple methods."""
        companies = []
        
        # Method 1: Try to find links by CSS selectors
        try:
            # Try multiple possible selectors for startup listings
            possible_selectors = [
                ".startup-item a", ".company-item a", ".search-results a", 
                "a[href*='startup']", "a[href*='company']", "a[href*='profile']",
                ".card a", ".result-item a", ".list-item a"
            ]
            
            for selector in possible_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for element in elements:
                        href = element.get_attribute("href")
                        if href and ("/startup/" in href or "/company/" in href or "/profile/" in href):
                            companies.append(href)
                    if companies:
                        break  # Stop if we found companies with this selector
        
        except Exception as e:
            logger.warning(f"Error finding company links via CSS: {e}")
        
        # Method 2: Find all links and filter by URL pattern
        if not companies:
            try:
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                for link in all_links:
                    try:
                        href = link.get_attribute("href")
                        if href and re.search(r"/(startup|company|profile|detail)/", href):
                            companies.append(href)
                    except StaleElementReferenceException:
                        continue
            except Exception as e:
                logger.warning(f"Error finding company links via general search: {e}")
        
        # Method 3: Use BeautifulSoup as backup
        if not companies:
            try:
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if re.search(r"/(startup|company|profile|detail)/", href):
                        full_url = urljoin(self.base_url, href)
                        companies.append(full_url)
            except Exception as e:
                logger.warning(f"Error finding company links via BeautifulSoup: {e}")
        
        # Remove duplicates and return
        unique_companies = list(set(companies))
        logger.info(f"Found {len(unique_companies)} unique company links")
        return unique_companies

    def extract_text_by_methods(self, label, methods_list):
        """Extract text using multiple fallback methods."""
        for method in methods_list:
            try:
                method_type = method["type"]
                selector = method["selector"]
                attribute = method.get("attribute", None)
                
                if method_type == "css":
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                elif method_type == "xpath":
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    continue
                
                for element in elements:
                    try:
                        if attribute:
                            value = element.get_attribute(attribute)
                        else:
                            value = element.text
                        
                        if value and value.strip():
                            logger.debug(f"Found {label}: {value[:30]}{'...' if len(value) > 30 else ''}")
                            return value.strip()
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"Method failed for {label}: {e}")
                continue
        
        # Try BeautifulSoup as a last resort
        try:
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            
            # Look for elements containing the label text
            label_elements = soup.find_all(string=re.compile(label, re.IGNORECASE))
            for element in label_elements:
                parent = element.parent
                if parent and parent.next_sibling:
                    text = parent.next_sibling.text.strip()
                    if text:
                        return text
        except Exception as e:
            logger.debug(f"BeautifulSoup fallback failed for {label}: {e}")
        
        return ""

    def extract_company_info(self, url):
        """Extract detailed company information from the company page."""
        logger.info(f"Extracting info from: {url}")
        
        if not self.get_page(url):
            logger.error(f"Failed to load company page: {url}")
            return None
        
        # Allow page to fully load
        time.sleep(2)
        
        company_data = {
            "Company Name": "",
            "Description": "",
            "Website": "",
            "Email": "",
            "Phone Number": "",
            "Region": "",
            "City": "",
            "Date of Establishment": "",
            "URL": url
        }
        
        # Company Name methods
        company_name_methods = [
            {"type": "css", "selector": "h1"},
            {"type": "css", "selector": ".company-name"},
            {"type": "css", "selector": ".startup-name"},
            {"type": "css", "selector": ".profile-header h1"},
            {"type": "xpath", "selector": "//h1"},
            {"type": "xpath", "selector": "//div[contains(@class, 'title')]"}
        ]
        company_data["Company Name"] = self.extract_text_by_methods("Company Name", company_name_methods)
        
        # Description methods
        description_methods = [
            {"type": "css", "selector": ".description"},
            {"type": "css", "selector": ".company-description"},
            {"type": "css", "selector": ".about"},
            {"type": "css", "selector": "p.description"},
            {"type": "xpath", "selector": "//div[contains(@class, 'description')]"},
            {"type": "xpath", "selector": "//section[contains(@class, 'about')]//p"}
        ]
        company_data["Description"] = self.extract_text_by_methods("Description", description_methods)
        
        # Website methods
        website_methods = [
            {"type": "css", "selector": "a[href^='http']", "attribute": "href"},
            {"type": "css", "selector": ".website a", "attribute": "href"},
            {"type": "css", "selector": ".url a", "attribute": "href"},
            {"type": "xpath", "selector": "//a[contains(@href, 'http')]", "attribute": "href"},
            {"type": "xpath", "selector": "//label[contains(text(), 'Website')]/following-sibling::*//a", "attribute": "href"}
        ]
        website = self.extract_text_by_methods("Website", website_methods)
        # Filter out social media and other irrelevant URLs
        if website and not any(domain in website for domain in ["facebook.com", "twitter.com", "linkedin.com", "instagram.com", "registroimprese.it"]):
            company_data["Website"] = website
        
        # Email methods - both visible text and mailto links
        email_methods = [
            {"type": "css", "selector": "a[href^='mailto:']", "attribute": "href"},
            {"type": "css", "selector": ".email"},
            {"type": "css", "selector": ".contact-email"},
            {"type": "xpath", "selector": "//a[contains(@href, 'mailto:')]", "attribute": "href"},
            {"type": "xpath", "selector": "//label[contains(text(), 'Email')]/following-sibling::*"}
        ]
        email = self.extract_text_by_methods("Email", email_methods)
        if email:
            # Extract email from mailto: link if needed
            email = email.replace("mailto:", "").strip()
            company_data["Email"] = email
        else:
            # Try to find email using regex in page source
            page_source = self.driver.page_source
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            emails = re.findall(email_pattern, page_source)
            if emails:
                company_data["Email"] = emails[0]
        
        # Phone Number methods
        phone_methods = [
            {"type": "css", "selector": ".phone"},
            {"type": "css", "selector": ".tel"},
            {"type": "css", "selector": ".contact-phone"},
            {"type": "xpath", "selector": "//label[contains(text(), 'Phone')]/following-sibling::*"},
            {"type": "xpath", "selector": "//div[contains(text(), 'Phone') or contains(text(), 'Tel')]/following-sibling::*"}
        ]
        company_data["Phone Number"] = self.extract_text_by_methods("Phone", phone_methods)
        
        # Region methods
        region_methods = [
            {"type": "css", "selector": ".region"},
            {"type": "css", "selector": ".location .region"},
            {"type": "xpath", "selector": "//label[contains(text(), 'Region')]/following-sibling::*"}
        ]
        company_data["Region"] = self.extract_text_by_methods("Region", region_methods)
        
        # City methods
        city_methods = [
            {"type": "css", "selector": ".city"},
            {"type": "css", "selector": ".location .city"},
            {"type": "xpath", "selector": "//label[contains(text(), 'City')]/following-sibling::*"}
        ]
        company_data["City"] = self.extract_text_by_methods("City", city_methods)
        
        # Date of Establishment methods
        date_methods = [
            {"type": "css", "selector": ".establishment-date"},
            {"type": "css", "selector": ".founded-date"},
            {"type": "css", "selector": ".foundation-date"},
            {"type": "xpath", "selector": "//label[contains(text(), 'Established') or contains(text(), 'Founded')]/following-sibling::*"}
        ]
        company_data["Date of Establishment"] = self.extract_text_by_methods("Establishment Date", date_methods)
        
        # Last ditch effort - extract all structured data from the page
        if not any(company_data.values()):
            try:
                # Try to extract all label-value pairs on the page
                labels = self.driver.find_elements(By.CSS_SELECTOR, "label, dt, th")
                for label in labels:
                    try:
                        label_text = label.text.strip().lower()
                        if not label_text:
                            continue
                            
                        # Find the corresponding value element
                        if label.tag_name == "label":
                            for_attr = label.get_attribute("for")
                            if for_attr:
                                value_elem = self.driver.find_element(By.ID, for_attr)
                            else:
                                value_elem = label.find_element(By.XPATH, "./following-sibling::*[1]")
                        elif label.tag_name == "dt":
                            value_elem = label.find_element(By.XPATH, "./following-sibling::dd[1]")
                        elif label.tag_name == "th":
                            idx = list(label.find_elements(By.XPATH, "./preceding-sibling::th")).index(label)
                            row = label.find_element(By.XPATH, "../following-sibling::tr[1]")
                            value_elem = row.find_elements(By.TAG_NAME, "td")[idx]
                        
                        value = value_elem.text.strip()
                        
                        # Map to our fields
                        if "name" in label_text and not company_data["Company Name"]:
                            company_data["Company Name"] = value
                        elif any(x in label_text for x in ["desc", "about", "activity"]) and not company_data["Description"]:
                            company_data["Description"] = value
                        elif "web" in label_text and not company_data["Website"]:
                            company_data["Website"] = value
                        elif "email" in label_text and not company_data["Email"]:
                            company_data["Email"] = value
                        elif any(x in label_text for x in ["phone", "tel", "number"]) and not company_data["Phone Number"]:
                            company_data["Phone Number"] = value
                        elif "region" in label_text and not company_data["Region"]:
                            company_data["Region"] = value
                        elif "city" in label_text and not company_data["City"]:
                            company_data["City"] = value
                        elif any(x in label_text for x in ["found", "estab", "date", "start"]) and not company_data["Date of Establishment"]:
                            company_data["Date of Establishment"] = value
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"Failed to extract structured data: {e}")
        
        return company_data

    def find_next_page_link(self):
        """Find the link to the next page of results."""
        next_page_url = None
        
        # Try multiple methods to find the next page link
        try:
            # Method 1: Common next page button patterns
            next_selectors = [
                ".pagination .next a", "a.next", "li.next a", 
                "a[rel='next']", ".pagination-next a",
                "a:contains('Next')", "a:contains('»')",
                "//a[contains(text(), 'Next')]",
                "//a[contains(@class, 'next')]"
            ]
            
            for selector in next_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    elif ":contains" in selector:
                        # For jQuery-like selectors, we need a workaround
                        text = selector.split("'")[1]
                        elements = self.driver.find_elements(By.TAG_NAME, "a")
                        elements = [e for e in elements if text in e.text]
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            href = element.get_attribute("href")
                            if href:
                                next_page_url = href
                                break
                    
                    if next_page_url:
                        break
                except Exception:
                    continue
            
            # Method 2: Try to find by page numbers
            if not next_page_url:
                # Find current active page number
                try:
                    active_page = self.driver.find_element(By.CSS_SELECTOR, ".pagination .active, .pagination .current")
                    active_page_num = int(active_page.text.strip())
                    
                    # Look for link to the next page number
                    next_page_elements = self.driver.find_elements(By.CSS_SELECTOR, f".pagination a")
                    for element in next_page_elements:
                        try:
                            page_num = int(element.text.strip())
                            if page_num == active_page_num + 1:
                                next_page_url = element.get_attribute("href")
                                break
                        except ValueError:
                            continue
                except Exception:
                    pass
        
        except Exception as e:
            logger.warning(f"Error finding next page link: {e}")
        
        return next_page_url

    def save_to_csv(self, data):
        """Save the extracted data to a CSV file."""
        if not data:
            logger.warning("No data to save")
            return False
        
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as file:
                fieldnames = list(data[0].keys())
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                for company in data:
                    writer.writerow(company)
            logger.info(f"Data saved to {self.output_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False

    def crawl(self, max_pages=5, companies_per_page=None):
        """Main crawling method that orchestrates the process."""
        current_url = self.base_url
        current_page = 1
        all_companies_data = []
        
        try:
            while current_url and current_page <= max_pages:
                logger.info(f"Crawling page {current_page}: {current_url}")
                
                # Load the page
                if not self.get_page(current_url):
                    logger.error(f"Failed to load page {current_page}: {current_url}")
                    break
                
                # Find all company links on the current page
                company_links = self.find_company_links()
                logger.info(f"Found {len(company_links)} company links on page {current_page}")
                
                # Limit companies per page if specified
                if companies_per_page and len(company_links) > companies_per_page:
                    company_links = company_links[:companies_per_page]
                
                # Process each company
                for link in company_links:
                    try:
                        company_data = self.extract_company_info(link)
                        if company_data and any(value for key, value in company_data.items() if key != "URL"):
                            all_companies_data.append(company_data)
                            logger.info(f"Successfully extracted data for: {company_data.get('Company Name', 'Unknown Company')}")
                        else:
                            logger.warning(f"No data extracted from {link}")
                    except Exception as e:
                        logger.error(f"Error processing company {link}: {e}")
                
                # Periodically save data in case of crash
                if current_page % 2 == 0 and all_companies_data:
                    self.save_to_csv(all_companies_data)
                    logger.info(f"Intermediate save completed after page {current_page}")
                
                # Find the next page link
                next_page_url = self.find_next_page_link()
                if next_page_url:
                    current_url = next_page_url
                    current_page += 1
                else:
                    logger.info("No next page found, crawling completed")
                    break
                
        except KeyboardInterrupt:
            logger.info("Crawling interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during crawling: {e}")
        finally:
            # Save all collected data
            if all_companies_data:
                self.save_to_csv(all_companies_data)
            
            # Close the browser
            try:
                self.driver.quit()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
            
            logger.info(f"Crawling completed. Collected data for {len(all_companies_data)} companies.")
            return all_companies_data


def main():
    base_url = "https://startup.registroimprese.it/index"  # Adjust the start URL if needed
    output_file = "startup_registry_data.csv"
    
    # Create the crawler with custom settings
    crawler = StartupRegistryCrawler(
        base_url=base_url,
        output_file=output_file,
        headless=False,  # Set to False to see the browser for debugging
        delay_range=(2, 5)  # Random delay between 2-5 seconds between requests
    )
    
    # Start crawling - adjust parameters as needed
    crawler.crawl(max_pages=10, companies_per_page=20)
    
    logger.info("Script execution completed")


if __name__ == "__main__":
    main()