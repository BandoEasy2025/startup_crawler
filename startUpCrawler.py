# startup_crawler.py - Main crawler script

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
import re
import json
import csv
import logging
from urllib.parse import urljoin
from datetime import datetime
import requests
from scrapy.http import FormRequest
from scrapy.utils.response import open_in_browser
import random
import time
import wget
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("startup_crawler.log"),
        logging.StreamHandler()
    ]
)

class StartupItem(scrapy.Item):
    """Define the item structure for storing startup data"""
    company_name = scrapy.Field()
    creation_date = scrapy.Field()
    region = scrapy.Field()
    city = scrapy.Field()
    description = scrapy.Field()
    email = scrapy.Field()
    phone = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()

class StartupRegistrySpider(scrapy.Spider):
    name = 'startup_registry'
    allowed_domains = ['startup.registroimprese.it']
    start_urls = ['https://startup.registroimprese.it/isin/home']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Add delay between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'HTTPERROR_ALLOW_ALL': True,  # Process pages that return errors
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
            'https': 'scrapy.core.downloader.handlers.http.HTTPDownloadHandler',
        },
        'ITEM_PIPELINES': {
            'scrapy.pipelines.files.FilesPipeline': 1,
            'startup_crawler.StartupCsvPipeline': 300,
        },
        'FILES_STORE': 'downloads',
        'CONCURRENT_REQUESTS': 1,  # Limit concurrent requests
        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            'startup_crawler.CustomHeadersMiddleware': 550,
        },
    }
    
    def __init__(self, *args, **kwargs):
        super(StartupRegistrySpider, self).__init__(*args, **kwargs)
        # Create downloads directory
        os.makedirs('downloads', exist_ok=True)
        # Track processed URLs
        self.processed_urls = set()
        # Company counter
        self.company_count = 0
        # Create document directory
        os.makedirs('documents', exist_ok=True)
        
        # Initialize CSV file
        self.csv_file = 'italian_startups.csv'
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'company_name', 'creation_date', 'region', 
                'city', 'description', 'email', 'phone'
            ])
            writer.writeheader()
    
    def parse(self, response):
        """Initial parse method for the homepage"""
        logging.info(f"Parsing homepage: {response.url}")
        
        # Save response for analysis
        self.save_response(response, 'homepage')
        
        # Try multiple search strategies
        search_urls = [
            'https://startup.registroimprese.it/isin/search?searchType=advanced',
            'https://startup.registroimprese.it/isin/search',
            'https://startup.registroimprese.it/isin/ricerca',
            'https://startup.registroimprese.it/isin/search?stato=A',
            'https://startup.registroimprese.it/isin/search?regione=',
        ]
        
        # First try to extract search form and submit it
        yield from self.try_submit_form(response)
        
        # Then try direct navigation to search URLs
        for url in search_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_search_results,
                errback=self.handle_error,
                meta={'dont_redirect': False, 'handle_httpstatus_list': [302, 301]}
            )
    
    def try_submit_form(self, response):
        """Attempt to find and submit search forms"""
        # Try to find search forms
        forms = response.css('form')
        logging.info(f"Found {len(forms)} forms on the page")
        
        for i, form in enumerate(forms):
            action = form.css('::attr(action)').get()
            method = form.css('::attr(method)').get() or 'GET'
            
            logging.info(f"Form {i+1}: action={action}, method={method}")
            
            # Extract form data
            formdata = {}
            for input_elem in form.css('input, select'):
                name = input_elem.css('::attr(name)').get()
                value = input_elem.css('::attr(value)').get() or ''
                if name:
                    formdata[name] = value
            
            # Add search parameters
            formdata.update({
                'searchType': 'advanced',
                'searchValue': '',
                'stato': 'A',
            })
            
            # Build form action URL
            action_url = urljoin(response.url, action) if action else 'https://startup.registroimprese.it/isin/search'
            
            logging.info(f"Submitting form to {action_url} with data: {formdata}")
            
            # Submit the form
            if method.upper() == 'POST':
                yield FormRequest(
                    url=action_url,
                    formdata=formdata,
                    callback=self.parse_search_results,
                    errback=self.handle_error,
                    meta={'dont_redirect': False}
                )
            else:
                yield scrapy.Request(
                    url=action_url,
                    callback=self.parse_search_results,
                    errback=self.handle_error,
                    meta={'formdata': formdata, 'dont_redirect': False}
                )
        
        # Also look for search buttons and links
        search_elements = response.css('a:contains("Cerca"), button:contains("Cerca"), a:contains("Search"), button:contains("Search")')
        for elem in search_elements:
            href = elem.css('::attr(href)').get()
            if href and not href.startswith('#'):
                url = urljoin(response.url, href)
                yield scrapy.Request(
                    url,
                    callback=self.parse_search_results,
                    errback=self.handle_error
                )
    
    def parse_search_results(self, response):
        """Parse the search results page"""
        logging.info(f"Parsing search results: {response.url}")
        
        # Save response for analysis
        self.save_response(response, 'search_results')
        
        # Extract company links using various selectors
        company_links = []
        selectors = [
            'a[href*="/company/"]', 'a[href*="/startup/"]', 
            'a[href*="/detail/"]', 'a[href*="/scheda/"]',
            'a[href*="/impresa/"]', 'a[href*="?id="]',
            'table tr td a', '.company-item a', '.result-item a',
            '.search-results a', '#search-results a'
        ]
        
        for selector in selectors:
            links = response.css(selector)
            for link in links:
                href = link.css('::attr(href)').get()
                if href:
                    absolute_url = urljoin(response.url, href)
                    if absolute_url not in company_links:
                        company_links.append(absolute_url)
        
        # If specific selectors didn't work, try a more general approach
        if not company_links:
            # Try to find any links with patterns that suggest company pages
            all_links = response.css('a[href]')
            for link in all_links:
                href = link.css('::attr(href)').get()
                if href and re.search(r'/(company|startup|detail|scheda|impresa)/', href, re.I):
                    absolute_url = urljoin(response.url, href)
                    if absolute_url not in company_links:
                        company_links.append(absolute_url)
        
        logging.info(f"Found {len(company_links)} potential company links")
        
        # Process each company URL
        for url in company_links:
            if url not in self.processed_urls:
                self.processed_urls.add(url)
                yield scrapy.Request(
                    url,
                    callback=self.parse_company,
                    errback=self.handle_error,
                    meta={'company_url': url}
                )
        
        # Check for next page
        next_page = self.get_next_page(response)
        if next_page:
            logging.info(f"Following next page: {next_page}")
            yield scrapy.Request(
                next_page,
                callback=self.parse_search_results,
                errback=self.handle_error
            )
    
    def get_next_page(self, response):
        """Extract the next page URL"""
        # Try various next page selectors
        selectors = [
            'a:contains("Next")', 'a:contains("Successivo")',
            'a:contains("Avanti")', 'a:contains("»")',
            'a.next', 'a[rel="next"]',
            'li.next a', 'a[aria-label="Next"]',
            'a[aria-label="Successivo"]'
        ]
        
        for selector in selectors:
            next_links = response.css(selector)
            for link in next_links:
                href = link.css('::attr(href)').get()
                if href:
                    return urljoin(response.url, href)
        
        # Try to find pagination links and determine the current page
        try:
            # Find active page element
            current_page = None
            active_selectors = ['li.active', 'li.selected', 'a.active', 'a.selected']
            for selector in active_selectors:
                active = response.css(selector)
                if active:
                    # Extract page number
                    current_text = active.css('::text').get()
                    if current_text and current_text.strip().isdigit():
                        current_page = int(current_text.strip())
                        break
            
            if current_page:
                # Look for next page number
                next_page_num = current_page + 1
                next_link = response.css(f'a:contains("{next_page_num}")')
                if next_link:
                    href = next_link.css('::attr(href)').get()
                    if href:
                        return urljoin(response.url, href)
        except Exception as e:
            logging.warning(f"Error finding pagination: {str(e)}")
        
        return None
    
    def parse_company(self, response):
        """Extract company information"""
        company_url = response.meta.get('company_url', response.url)
        logging.info(f"Parsing company page: {company_url}")
        
        # Save response for analysis
        self.save_response(response, f'company_{self.company_count + 1}')
        
        # Create new item
        item = StartupItem()
        
        # 1. Extract company name
        name_selectors = [
            'h1::text', 'h2::text', 
            '.company-name::text', '#company-name::text',
            'div.header h1::text', 'div.intestazione h1::text',
            '.ragione-sociale::text'
        ]
        
        item['company_name'] = self.extract_with_selectors(response, name_selectors) or "Unknown Company"
        
        # 2. Extract creation date
        date_labels = [
            'Data Costituzione', 'Data di costituzione', 'Costituzione', 
            'Foundation Date', 'Data iscrizione', 'Costituita il'
        ]
        
        item['creation_date'] = self.extract_labeled_field(response, date_labels)
        
        # If not found, try regex
        if not item['creation_date']:
            date_patterns = [
                r'(?:costituzione|costituita|foundation|created).*?(\d{2}[/.-]\d{2}[/.-]\d{4})',
                r'(?:costituzione|costituita|foundation|created).*?(\d{4}[/.-]\d{2}[/.-]\d{2})'
            ]
            
            for pattern in date_patterns:
                matches = re.findall(pattern, response.text, re.I)
                if matches:
                    item['creation_date'] = matches[0]
                    break
        
        # 3. Extract region
        region_labels = ['Regione', 'Region', 'Territory']
        item['region'] = self.extract_labeled_field(response, region_labels)
        
        # 4. Extract city
        city_labels = ['Comune', 'Città', 'City', 'Località', 'Location', 'Sede']
        item['city'] = self.extract_labeled_field(response, city_labels)
        
        # 5. Extract description
        desc_selectors = [
            '.description::text', '#description::text',
            '.company-description::text', '#company-description::text',
            '.about::text', '#about::text', '.profile::text', '#profile::text'
        ]
        
        item['description'] = self.extract_with_selectors(response, desc_selectors)
        
        # If no description found, look for paragraphs in main content
        if not item['description']:
            main_selectors = ['main', 'article', '.content', '#content', '.main-content', '#main-content']
            for selector in main_selectors:
                paragraphs = response.css(f'{selector} p::text').getall()
                if paragraphs:
                    desc_text = " ".join([p.strip() for p in paragraphs[:3] if p.strip()])
                    if desc_text:
                        item['description'] = desc_text
                        break
        
        # 6. Extract email
        # First try mailto links
        email_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        for link in email_links:
            email = link.replace('mailto:', '').strip()
            if '@' in email and '.' in email:  # Basic validation
                item['email'] = email
                break
        
        # If not found, try regex
        if not item.get('email'):
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            email_matches = re.findall(email_pattern, response.text)
            if email_matches:
                # Filter out common non-company emails
                filtered_emails = [e for e in email_matches if not any(domain in e.lower() for domain in ['example.com', 'gmail.com', 'libero.it', 'hotmail'])]
                if filtered_emails:
                    item['email'] = filtered_emails[0]
                else:
                    item['email'] = email_matches[0]
        
        # 7. Extract phone number
        phone_labels = ['Telefono', 'Tel', 'Phone', 'Contatto', 'Contact']
        item['phone'] = self.extract_labeled_field(response, phone_labels)
        
        # If not found, try regex
        if not item.get('phone'):
            phone_patterns = [
                r'\+39\s?\d{10}',
                r'\+39\s?\d{3}[-\s]?\d{7}',
                r'\+39\s?\d{2}[-\s]?\d{8}',
                r'\+39\s?\d{3}[-\s]?\d{3}[-\s]?\d{4}',
                r'\+39\s?\d{3}[-\s]?\d{4}[-\s]?\d{3}',
                r'0\d{1,3}[-\s]?\d{6,7}',
                r'3\d{2}[-\s]?\d{6,7}'  # Mobile
            ]
            
            for pattern in phone_patterns:
                matches = re.findall(pattern, response.text)
                if matches:
                    item['phone'] = matches[0].strip()
                    break
        
        # 8. Look for downloadable files
        file_urls = []
        
        # Look for PDF, DOC, DOCX, XLS, XLSX links
        file_selectors = [
            'a[href$=".pdf"]::attr(href)',
            'a[href$=".doc"]::attr(href)',
            'a[href$=".docx"]::attr(href)',
            'a[href$=".xls"]::attr(href)',
            'a[href$=".xlsx"]::attr(href)',
            'a[href*="download"]::attr(href)',
            'a[href*="allegato"]::attr(href)',
            'a[href*="attachment"]::attr(href)'
        ]
        
        for selector in file_selectors:
            urls = response.css(selector).getall()
            for url in urls:
                if url:
                    absolute_url = urljoin(response.url, url)
                    if absolute_url not in file_urls:
                        file_urls.append(absolute_url)
        
        if file_urls:
            item['file_urls'] = file_urls
            
            # Also download files directly with wget as a backup
            for file_url in file_urls:
                try:
                    filename = os.path.basename(file_url)
                    if not filename or filename.isspace():
                        filename = f"file_{self.company_count + 1}_{random.randint(1000, 9999)}.pdf"
                        
                    save_path = os.path.join('documents', filename)
                    logging.info(f"Downloading file: {file_url} to {save_path}")
                    
                    # Download the file with wget (more reliable for some sites)
                    wget.download(file_url, out=save_path)
                    
                except Exception as e:
                    logging.error(f"Error downloading file {file_url}: {str(e)}")
        
        # Increment company counter
        self.company_count += 1
        
        # Save to CSV directly as a backup
        self.save_to_csv(dict(item))
        
        # Return the item for pipeline processing
        logging.info(f"Extracted data for company #{self.company_count}: {item['company_name']}")
        return item
    
    def extract_with_selectors(self, response, selectors):
        """Extract content using multiple selectors"""
        for selector in selectors:
            try:
                content = response.css(selector).get()
                if content and content.strip():
                    return content.strip()
            except Exception:
                continue
                
        return None
    
    def extract_labeled_field(self, response, labels):
        """Extract content associated with a label"""
        # Try various approaches to find labeled content
        
        # Method 1: Look for elements containing the label
        for label in labels:
            # Try elements containing the exact label
            elements = response.xpath(f'//*[contains(text(), "{label}")]')
            for element in elements:
                # Check if the element itself contains the value
                full_text = element.css('::text').get() or ''
                if ':' in full_text:
                    parts = full_text.split(':', 1)
                    if len(parts) > 1 and parts[1].strip():
                        return parts[1].strip()
                
                # Look for next sibling or next element
                next_text = element.xpath('./following-sibling::*[1]//text()').get()
                if next_text and next_text.strip():
                    return next_text.strip()
                
                # Look for parent's next child
                parent_next = element.xpath('../*[position()=count(../*[.=current()])+1]//text()').get()
                if parent_next and parent_next.strip():
                    return parent_next.strip()
        
        # Method 2: Look for table structures
        for label in labels:
            # Find table cells containing the label
            cells = response.xpath(f'//td[contains(text(), "{label}")]')
            for cell in cells:
                # Try to get the next cell in the same row
                next_cell = cell.xpath('./following-sibling::td[1]//text()').get()
                if next_cell and next_cell.strip():
                    return next_cell.strip()
        
        # Method a third approach with CSS selectors
        for label in labels:
            # Look for definition lists
            dt = response.css(f'dt:contains("{label}")')
            if dt:
                dd = dt.xpath('./following-sibling::dd[1]//text()').get()
                if dd and dd.strip():
                    return dd.strip()
        
        return None
    
    def save_response(self, response, name):
        """Save response for debugging"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"debug_{name}_{timestamp}.html"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logging.info(f"Saved response to {filename}")
    
    def save_to_csv(self, item_dict):
        """Save item to CSV file directly"""
        # Extract only the relevant fields for CSV
        csv_data = {field: item_dict.get(field, '') for field in [
            'company_name', 'creation_date', 'region', 
            'city', 'description', 'email', 'phone'
        ]}
        
        # Write to CSV file
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_data.keys())
            writer.writerow(csv_data)
        
        logging.info(f"Saved data to CSV for: {csv_data['company_name']}")
    
    def handle_error(self, failure):
        """Handle request errors"""
        logging.error(f"Request failed: {failure.value}")
        request = failure.request
        if hasattr(failure.value, 'response') and failure.value.response:
            response = failure.value.response
            logging.error(f"Status code: {response.status}")
            self.save_response(response, f"error_{response.status}")

class StartupCsvPipeline:
    """Pipeline for saving startup data to CSV"""
    
    def __init__(self):
        self.csv_file = 'italian_startups.csv'
        self.ensure_csv_exists()
    
    def ensure_csv_exists(self):
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'company_name', 'creation_date', 'region', 
                    'city', 'description', 'email', 'phone'
                ])
                writer.writeheader()
    
    def process_item(self, item, spider):
        # Write to CSV file
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'company_name', 'creation_date', 'region', 
                'city', 'description', 'email', 'phone'
            ])
            csv_item = {field: item.get(field, '') for field in writer.fieldnames}
            writer.writerow(csv_item)
        
        return item

class CustomHeadersMiddleware:
    """Middleware to add custom headers to requests"""
    
    def process_request(self, request, spider):
        # Add headers to appear more like a regular browser
        request.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Chromium";v="122", "Google Chrome";v="122"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://startup.registroimprese.it/isin/home'
        })
        return None

if __name__ == '__main__':
    # Configure crawler settings
    settings = get_project_settings()
    settings.update({
        'BOT_NAME': 'startup_registry_crawler',
        'LOG_LEVEL': 'INFO',
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'HTTPERROR_ALLOW_ALL': True,
        'ITEM_PIPELINES': {
            'scrapy.pipelines.files.FilesPipeline': 1,
            '__main__.StartupCsvPipeline': 300,
        },
        'FILES_STORE': 'downloads',
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.CustomHeadersMiddleware': 550,
        },
    })
    
    # Run the crawler
    process = CrawlerProcess(settings)
    process.crawl(StartupRegistrySpider)
    process.start()