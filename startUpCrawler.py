import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

class ItalianStartupCrawler:
    def __init__(self):
        self.base_url = "https://startup.registroimprese.it/isin/home"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,it;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.output_file = "italian_startups.csv"
        # Define CSV headers
        self.fieldnames = [
            'company_name', 
            'creation_date', 
            'region', 
            'city', 
            'description', 
            'email', 
            'phone'
        ]
        
    def get_soup(self, url, method='get', data=None):
        """Get BeautifulSoup object from a URL"""
        try:
            if method.lower() == 'post':
                response = self.session.post(url, headers=self.headers, data=data)
            else:
                response = self.session.get(url, headers=self.headers)
            
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            return None
    
    def initialize_csv(self):
        """Create CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.output_file):
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
    
    def save_to_csv(self, company_data):
        """Save company data to CSV file"""
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(company_data)
    
    def submit_search(self):
        """Submit the search form to get results"""
        logging.info("Accessing home page...")
        home_soup = self.get_soup(self.base_url)
        if not home_soup:
            logging.error("Failed to access home page")
            return None
        
        search_url = "https://startup.registroimprese.it/isin/search"
        logging.info(f"Submitting search at {search_url}")
        
        # Get the search form
        form_data = {
            # These values may need to be adjusted based on website inspection
            'searchType': 'advanced',
            'searchValue': '',
            # Add any other necessary form fields
        }
        
        return self.get_soup(search_url, method='post', data=form_data)
    
    def get_company_urls(self, results_soup):
        """Extract company URLs from search results page"""
        if not results_soup:
            return []
        
        company_links = []
        # Find all company links (adjust selector based on actual HTML structure)
        links = results_soup.find_all('a', href=lambda href: href and ('/company/' in href or '/startup/' in href))
        
        for link in links:
            href = link.get('href', '')
            if href.startswith('http'):
                company_url = href
            else:
                company_url = 'https://startup.registroimprese.it' + href
            company_links.append(company_url)
        
        return company_links
    
    def get_next_page_url(self, soup):
        """Get URL of the next page of results"""
        if not soup:
            return None
        
        # Look for pagination links (adjust selectors based on actual HTML)
        next_link = soup.find('a', text='Next') or soup.find('a', text='Successivo') or \
                    soup.find('a', class_='next') or soup.find('a', rel='next')
        
        if next_link and next_link.get('href'):
            href = next_link['href']
            if href.startswith('http'):
                return href
            else:
                return 'https://startup.registroimprese.it' + href
        
        return None
    
    def extract_company_info(self, company_url):
        """Extract required company information"""
        soup = self.get_soup(company_url)
        if not soup:
            return None
        
        company_info = {
            'company_name': '',
            'creation_date': '',
            'region': '',
            'city': '',
            'description': '',
            'email': '',
            'phone': ''
        }
        
        # The following selectors need to be adjusted based on actual HTML structure
        
        # Company name
        name_element = soup.find('h1') or soup.find('div', class_='company-name')
        if name_element:
            company_info['company_name'] = name_element.text.strip()
        
        # Creation date - look for elements containing these labels
        date_labels = ['Data Costituzione', 'Data di costituzione', 'Costituzione', 'Foundation Date']
        for label in date_labels:
            element = soup.find(lambda tag: tag.name and tag.string and label in tag.string)
            if element:
                value = self._get_next_text(element)
                if value:
                    company_info['creation_date'] = value
                    break
        
        # Region
        region_labels = ['Regione', 'Region']
        for label in region_labels:
            element = soup.find(lambda tag: tag.name and tag.string and label in tag.string)
            if element:
                value = self._get_next_text(element)
                if value:
                    company_info['region'] = value
                    break
        
        # City
        city_labels = ['Comune', 'Città', 'City']
        for label in city_labels:
            element = soup.find(lambda tag: tag.name and tag.string and label in tag.string)
            if element:
                value = self._get_next_text(element)
                if value:
                    company_info['city'] = value
                    break
        
        # Description
        desc_element = soup.find('div', class_='description') or soup.find('div', id='description')
        if desc_element:
            company_info['description'] = desc_element.text.strip()
        
        # Email - look for mailto links
        email_element = soup.find('a', href=lambda href: href and 'mailto:' in href)
        if email_element:
            email = email_element.text.strip()
            if not email and 'href' in email_element.attrs:
                email = email_element['href'].replace('mailto:', '')
            company_info['email'] = email
        
        # Phone
        phone_labels = ['Telefono', 'Tel', 'Phone']
        for label in phone_labels:
            element = soup.find(lambda tag: tag.name and tag.string and label in tag.string)
            if element:
                value = self._get_next_text(element)
                if value:
                    company_info['phone'] = value
                    break
        
        return company_info
    
    def _get_next_text(self, element):
        """Helper to get text from next element or sibling"""
        next_elem = element.find_next() or element.next_sibling
        if next_elem:
            return next_elem.text.strip()
        return ''
    
    def crawl(self):
        """Main crawling function"""
        logging.info("Starting crawler")
        self.initialize_csv()
        
        # Step 1: Submit search to get results
        results_soup = self.submit_search()
        if not results_soup:
            logging.error("Failed to get search results")
            return
        
        page_num = 1
        companies_processed = 0
        
        # Step 2: Process each page of results
        while results_soup:
            logging.info(f"Processing page {page_num}")
            
            # Step 3: Get company URLs from current page
            company_urls = self.get_company_urls(results_soup)
            logging.info(f"Found {len(company_urls)} companies on page {page_num}")
            
            # Step 4: Process each company
            for i, company_url in enumerate(company_urls):
                logging.info(f"Processing company {i+1}/{len(company_urls)} on page {page_num}")
                
                company_info = self.extract_company_info(company_url)
                if company_info:
                    self.save_to_csv(company_info)
                    companies_processed += 1
                    logging.info(f"Saved data for: {company_info['company_name']}")
                
                # Add delay to avoid overloading the server
                time.sleep(random.uniform(1, 3))
            
            # Step 5: Move to next page if available
            next_url = self.get_next_page_url(results_soup)
            if next_url:
                logging.info(f"Moving to page {page_num + 1}")
                page_num += 1
                time.sleep(random.uniform(2, 5))  # Delay between pages
                results_soup = self.get_soup(next_url)
            else:
                logging.info("No more pages found")
                break
        
        logging.info(f"Crawling completed. Processed {companies_processed} companies.")
        logging.info(f"Data saved to {self.output_file}")

if __name__ == "__main__":
    crawler = ItalianStartupCrawler()
    crawler.crawl()