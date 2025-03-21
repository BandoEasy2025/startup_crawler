import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import os
import re
import logging
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

class ItalianStartupCrawler:
    def __init__(self):
        self.base_url = "https://startup.registroimprese.it/isin/home"
        self.search_url = "https://startup.registroimprese.it/isin/search"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,it;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Origin': 'https://startup.registroimprese.it',
            'Referer': 'https://startup.registroimprese.it/isin/home'
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
        
    def get_soup(self, url, method='get', data=None, params=None):
        """Get BeautifulSoup object from a URL with detailed error handling"""
        try:
            logging.info(f"Requesting {url} with method {method}")
            if method.lower() == 'post':
                response = self.session.post(url, headers=self.headers, data=data)
            else:
                response = self.session.get(url, headers=self.headers, params=params)
            
            logging.info(f"Response status code: {response.status_code}")
            
            # Debug response headers and cookies
            logging.info(f"Response cookies: {response.cookies.get_dict()}")
            logging.info(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            
            # Save the HTML for debugging if needed
            with open("last_response.html", "w", encoding="utf-8") as f:
                f.write(response.text)
                
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Response content: {e.response.text[:500]}...")
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
    
    def get_csrf_token(self, soup):
        """Extract CSRF token from the page"""
        meta = soup.find('meta', attrs={'name': '_csrf'})
        if meta and meta.get('content'):
            return meta.get('content')
        
        # Alternative method
        inputs = soup.find_all('input', attrs={'name': '_csrf'})
        for input_field in inputs:
            if input_field.get('value'):
                return input_field.get('value')
                
        return None
        
    def extract_form_data(self, soup):
        """Extract all form inputs for accurate form submission"""
        form = soup.find('form', attrs={'action': lambda x: x and 'search' in x.lower()})
        if not form:
            form = soup.find('form')  # Try to find any form
            
        if not form:
            logging.error("No search form found on the page")
            return {}
            
        form_data = {}
        for input_field in form.find_all('input'):
            name = input_field.get('name')
            value = input_field.get('value', '')
            if name:  # Only add if name attribute exists
                form_data[name] = value
                
        # Add additional form parameters that might be required
        form_data['searchValue'] = ''
        form_data['searchType'] = 'advanced'
        
        return form_data
    
    def submit_search(self):
        """Submit the search form to get results with detailed error handling"""
        logging.info("Accessing home page...")
        home_soup = self.get_soup(self.base_url)
        if not home_soup:
            logging.error("Failed to access home page")
            return None
        
        # Extract CSRF token if present
        csrf_token = self.get_csrf_token(home_soup)
        if csrf_token:
            logging.info(f"Found CSRF token: {csrf_token}")
            self.headers['X-CSRF-TOKEN'] = csrf_token
        
        # Extract form data
        form_data = self.extract_form_data(home_soup)
        logging.info(f"Extracted form data: {form_data}")
        
        # Try a more comprehensive approach
        params = {
            'searchType': 'advanced',
            'searchValue': '',
            'ateco': '',
            'comune': '',
            'provincia': '',
            'regione': '',
            'stato': 'A'  # A might mean 'Active'
        }
        
        # First try with GET parameters
        results_soup = self.get_soup(self.search_url, params=params)
        
        # If that doesn't work, try POST with form data
        if not results_soup or "No results" in results_soup.text:
            logging.info("GET search failed or returned no results, trying POST...")
            results_soup = self.get_soup(self.search_url, method='post', data=form_data)
        
        # Debug the results
        if results_soup:
            with open("search_results.html", "w", encoding="utf-8") as f:
                f.write(str(results_soup))
                
        return results_soup
    
    def get_company_urls(self, results_soup):
        """Extract company URLs from search results page with multiple selector attempts"""
        if not results_soup:
            return []
        
        company_links = []
        
        # Try various selectors that might contain company links
        selectors = [
            # Common patterns for company links
            'a[href*="/company/"]',
            'a[href*="/startup/"]',
            'a[href*="/detail/"]',
            'a[href*="/scheda/"]',
            'a[href*="/impresa/"]',
            # Class or ID based selectors
            '.company-item a',
            '.search-results a',
            '.result-item a',
            '#search-results a',
            # Look for table rows
            'table tr td a'
        ]
        
        for selector in selectors:
            logging.info(f"Trying selector: {selector}")
            try:
                elements = results_soup.select(selector)
                if elements:
                    logging.info(f"Found {len(elements)} elements with selector {selector}")
                    for element in elements:
                        href = element.get('href', '')
                        if href and not href.startswith('#') and not href.startswith('javascript:'):
                            if not href.startswith('http'):
                                href = urljoin(self.base_url, href)
                            company_links.append(href)
            except Exception as e:
                logging.error(f"Error with selector {selector}: {str(e)}")
        
        # If the above selectors didn't work, try a more general approach
        if not company_links:
            logging.info("No links found with specific selectors, trying generic approach")
            for a_tag in results_soup.find_all('a', href=True):
                href = a_tag['href']
                # Look for patterns that might indicate a company detail page
                if re.search(r'/(company|startup|detail|scheda|impresa)/', href, re.I) or \
                   re.search(r'\bid=\d+', href, re.I):
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    company_links.append(href)
        
        # Remove duplicates
        company_links = list(set(company_links))
        logging.info(f"Found {len(company_links)} unique company links")
        
        return company_links
    
    def get_next_page_url(self, soup):
        """Get URL of the next page of results using multiple approaches"""
        if not soup:
            return None
        
        # Try multiple approaches to find the next page link
        
        # 1. Look for elements containing 'Next' or 'Successivo'
        next_elements = [
            soup.find('a', string=lambda s: s and ('Next' in s or 'Successivo' in s or 'Avanti' in s or '»' in s)),
            soup.find('a', attrs={'aria-label': lambda s: s and ('Next' in s or 'Successivo' in s or 'Avanti' in s)}),
            soup.find('a', attrs={'class': lambda c: c and ('next' in c or 'successivo' in c or 'avanti' in c)}),
            soup.find('a', attrs={'rel': 'next'}),
            soup.find('li', attrs={'class': 'next'})
        ]
        
        for elem in next_elements:
            if elem:
                if elem.name != 'a' and elem.find('a'):
                    elem = elem.find('a')
                
                if elem.name == 'a' and elem.get('href'):
                    href = elem['href']
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    logging.info(f"Found next page link: {href}")
                    return href
        
        # 2. Look for pagination elements
        pagination = soup.find('ul', attrs={'class': lambda c: c and ('pagination' in c)})
        if pagination:
            # Find the active page
            active = pagination.find('li', attrs={'class': lambda c: c and ('active' in c)})
            if active:
                # Try to find the next sibling with a link
                next_li = active.find_next_sibling('li')
                if next_li and next_li.find('a', href=True):
                    href = next_li.find('a')['href']
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    logging.info(f"Found next page link from pagination: {href}")
                    return href
        
        logging.info("No next page link found")
        return None
    
    def extract_company_info(self, company_url):
        """Extract required company information with detailed debugging"""
        logging.info(f"Extracting data from: {company_url}")
        soup = self.get_soup(company_url)
        if not soup:
            logging.error(f"Failed to load company page: {company_url}")
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
        
        # Debug the page structure
        page_text = soup.get_text()
        logging.info(f"Page content length: {len(page_text)}")
        logging.info(f"Page content sample: {page_text[:200]}...")
        
        # Save company page for debugging
        with open(f"company_page_{int(time.time())}.html", "w", encoding="utf-8") as f:
            f.write(str(soup))
        
        # Extract data using multiple approaches
        
        # 1. Company name
        name_selectors = [
            'h1', 'h2', '.company-name', '#company-name',
            'div.header h1', 'div.intestazione h1',
            'div.company-header h1', '.ragione-sociale'
        ]
        
        for selector in name_selectors:
            element = soup.select_one(selector)
            if element and element.text.strip():
                company_info['company_name'] = element.text.strip()
                logging.info(f"Found company name: {company_info['company_name']}")
                break
        
        # 2. Creation date - look for elements containing date-related labels
        date_labels = [
            'Data Costituzione', 'Data di costituzione', 'Costituzione', 
            'Foundation Date', 'Data iscrizione', 'Data', 'Anno fondazione',
            'Costituita il', 'Established on', 'Established in'
        ]
        
        for label in date_labels:
            # Method 1: Look for text containing the label
            elements = soup.find_all(lambda tag: tag.name and tag.string and 
                                    label.lower() in tag.get_text().lower())
            
            for element in elements:
                # Try to find the value in the next sibling or child
                value = self._get_adjacent_text(element)
                if value and re.search(r'\d{2}[/.-]\d{2}[/.-]\d{4}|\d{4}', value):  # Date pattern
                    company_info['creation_date'] = value.strip()
                    logging.info(f"Found creation date: {company_info['creation_date']}")
                    break
            
            if company_info['creation_date']:
                break
        
        # 3. Region
        region_labels = ['Regione', 'Region', 'Territory']
        for label in region_labels:
            elements = soup.find_all(lambda tag: tag.name and tag.string and 
                                    label.lower() in tag.get_text().lower())
            
            for element in elements:
                value = self._get_adjacent_text(element)
                if value:
                    company_info['region'] = value.strip()
                    logging.info(f"Found region: {company_info['region']}")
                    break
            
            if company_info['region']:
                break
        
        # 4. City
        city_labels = ['Comune', 'Città', 'City', 'Località', 'Location', 'Sede']
        for label in city_labels:
            elements = soup.find_all(lambda tag: tag.name and tag.string and 
                                    label.lower() in tag.get_text().lower())
            
            for element in elements:
                value = self._get_adjacent_text(element)
                if value:
                    company_info['city'] = value.strip()
                    logging.info(f"Found city: {company_info['city']}")
                    break
            
            if company_info['city']:
                break
        
        # 5. Description
        desc_selectors = [
            '.description', '#description', '.company-description', 
            '#company-description', '.about', '#about', '.profile', 
            '#profile', '.activity', '#activity'
        ]
        
        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element and element.text.strip():
                company_info['description'] = element.text.strip()
                logging.info(f"Found description (length: {len(company_info['description'])})")
                break
        
        # 6. Email - look for mailto links and email patterns
        # Method 1: mailto links
        email_links = soup.find_all('a', href=lambda href: href and 'mailto:' in href)
        for link in email_links:
            email = link.get('href', '').replace('mailto:', '').strip()
            if '@' in email:
                company_info['email'] = email
                logging.info(f"Found email: {company_info['email']}")
                break
        
        # Method 2: Look for email pattern in text
        if not company_info['email']:
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            email_matches = re.findall(email_pattern, page_text)
            if email_matches:
                company_info['email'] = email_matches[0]
                logging.info(f"Found email with regex: {company_info['email']}")
        
        # 7. Phone number
        phone_labels = ['Telefono', 'Tel', 'Phone', 'Contatto', 'Contact']
        for label in phone_labels:
            elements = soup.find_all(lambda tag: tag.name and tag.string and 
                                    label.lower() in tag.get_text().lower())
            
            for element in elements:
                value = self._get_adjacent_text(element)
                # Look for phone number patterns
                if value and re.search(r'[\d\s+().-]{7,}', value):
                    company_info['phone'] = value.strip()
                    logging.info(f"Found phone: {company_info['phone']}")
                    break
            
            if company_info['phone']:
                break
        
        # Method 2: Look for phone number pattern in text
        if not company_info['phone']:
            phone_pattern = r'(?:\+\d{1,3})?(?:\s|\()?\d{2,4}(?:\s|\))?\s?\d{3,4}(?:\s|-|–)?\d{3,4}'
            phone_matches = re.findall(phone_pattern, page_text)
            if phone_matches:
                company_info['phone'] = phone_matches[0]
                logging.info(f"Found phone with regex: {company_info['phone']}")
        
        return company_info
    
    def _get_adjacent_text(self, element):
        """Helper to get text from adjacent elements with multiple strategies"""
        # Strategy 1: Get text from the next sibling
        next_sibling = element.next_sibling
        if next_sibling and isinstance(next_sibling, str) and next_sibling.strip():
            return next_sibling.strip()
        
        # Strategy 2: Get text from the next element
        next_elem = element.find_next()
        if next_elem and next_elem.string and next_elem.string.strip():
            return next_elem.string.strip()
        
        # Strategy 3: Look for specific value containers
        parent = element.parent
        if parent:
            value_containers = parent.find_all(['span', 'div', 'td'], class_=lambda c: c and 'value' in c)
            for container in value_containers:
                if container.text.strip():
                    return container.text.strip()
        
        # Strategy 4: For table structures, look for the next cell
        if element.name == 'td' or element.parent.name == 'td':
            td = element if element.name == 'td' else element.parent
            next_td = td.find_next('td')
            if next_td and next_td.text.strip():
                return next_td.text.strip()
        
        return ''
    
    def crawl(self):
        """Main crawling function with robust error handling"""
        logging.info("Starting crawler")
        self.initialize_csv()
        
        # Step 1: Submit search to get results
        results_soup = self.submit_search()
        if not results_soup:
            logging.error("Failed to get search results")
            return
        
        # Check if results page indicates no results
        if "No results" in results_soup.get_text() or "Nessun risultato" in results_soup.get_text():
            logging.warning("Search returned no results")
            logging.info("Trying alternative search approach...")
            
            # Try alternative search approach with minimal parameters
            results_soup = self.get_soup(self.search_url, params={'stato': 'A'})
            
            if not results_soup or "No results" in results_soup.get_text():
                logging.error("All search attempts failed to return results")
                return
        
        page_num = 1
        companies_processed = 0
        total_companies_found = 0
        max_pages = 50  # Limit to prevent infinite loops
        
        # Step 2: Process each page of results
        while results_soup and page_num <= max_pages:
            logging.info(f"Processing page {page_num}")
            
            # Step 3: Get company URLs from current page
            company_urls = self.get_company_urls(results_soup)
            logging.info(f"Found {len(company_urls)} companies on page {page_num}")
            total_companies_found += len(company_urls)
            
            # Step 4: Process each company
            for i, company_url in enumerate(company_urls):
                try:
                    logging.info(f"Processing company {i+1}/{len(company_urls)} on page {page_num}")
                    
                    company_info = self.extract_company_info(company_url)
                    if company_info:
                        self.save_to_csv(company_info)
                        companies_processed += 1
                        logging.info(f"Saved data for: {company_info['company_name']}")
                    else:
                        logging.warning(f"Failed to extract info from: {company_url}")
                    
                    # Add delay to avoid overloading the server
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logging.error(f"Error processing company {company_url}: {str(e)}")
                    continue
            
            # Step 5: Move to next page if available
            next_url = self.get_next_page_url(results_soup)
            if next_url:
                logging.info(f"Moving to page {page_num + 1}")
                page_num += 1
                time.sleep(random.uniform(2, 5))  # Delay between pages
                results_soup = self.get_soup(next_url)
                
                # Save the page for debugging
                if results_soup:
                    with open(f"results_page_{page_num}.html", "w", encoding="utf-8") as f:
                        f.write(str(results_soup))
            else:
                logging.info("No more pages found")
                break
        
        if total_companies_found == 0:
            logging.warning("No companies were found. The website structure might have changed.")
            logging.info("Please check the HTML files saved for debugging.")
        
        logging.info(f"Crawling completed. Found {total_companies_found} companies, processed {companies_processed}.")
        logging.info(f"Data saved to {self.output_file}")

if __name__ == "__main__":
    crawler = ItalianStartupCrawler()
    crawler.crawl()