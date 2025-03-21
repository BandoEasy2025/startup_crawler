# improved_startup_crawler.py - Optimized crawler script

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, Join, MapCompose
from scrapy.exporters import CsvItemExporter
from scrapy import signals
from scrapy.http import FormRequest
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
from scrapy.dupefilters import RFPDupeFilter
from urllib.parse import urljoin, urlparse
import os
import re
import json
import logging
from datetime import datetime
import time
import hashlib
from pathlib import Path
from w3lib.html import remove_tags

# Set up logging with proper configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("startup_crawler.log"),
        logging.StreamHandler()
    ]
)

class StartupItem(scrapy.Item):
    """Define the item structure for storing startup data with cleaner field definitions"""
    company_name = scrapy.Field()
    creation_date = scrapy.Field()
    region = scrapy.Field()
    city = scrapy.Field()
    description = scrapy.Field()
    email = scrapy.Field()
    phone = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()


class StartupLoader(ItemLoader):
    """Custom item loader with processors for clean data extraction"""
    default_output_processor = TakeFirst()
    description_out = Join(' ')
    
    # Clean text by removing excessive whitespace
    company_name_in = MapCompose(str.strip, lambda x: re.sub(r'\s+', ' ', x))
    description_in = MapCompose(str.strip, lambda x: re.sub(r'\s+', ' ', x))
    
    # Clean date formats
    creation_date_in = MapCompose(
        str.strip,
        lambda x: re.sub(r'[^\d/.-]', '', x)
    )
    
    # Clean email addresses
    email_in = MapCompose(
        str.strip,
        lambda x: x.replace('mailto:', ''),
        lambda x: x if '@' in x and '.' in x.split('@')[-1] else None
    )
    
    # Clean phone numbers
    phone_in = MapCompose(
        str.strip,
        lambda x: re.sub(r'[^\d+\s-]', '', x)
    )


class CacheURLFilter(RFPDupeFilter):
    """Enhanced URL filter with smarter caching strategy"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_urls = set()
        self.company_fingerprints = set()
    
    def request_seen(self, request):
        # Get URL without query parameters for better deduplication
        parsed_url = urlparse(request.url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        
        # Check if we've already seen this URL
        if base_url in self.seen_urls:
            return True
        
        # For company pages, also check content fingerprints
        if any(marker in request.url for marker in ['/company/', '/startup/', '/detail/', '/scheda/', '/impresa/']):
            if 'company_fingerprint' in request.meta and request.meta['company_fingerprint'] in self.company_fingerprints:
                return True
            
        # Mark URL as seen
        self.seen_urls.add(base_url)
        return super().request_seen(request)
    
    def register_company_fingerprint(self, fingerprint):
        """Register a company content fingerprint to avoid duplicates"""
        self.company_fingerprints.add(fingerprint)


class StartupRegistrySpider(scrapy.Spider):
    name = 'startup_registry'
    allowed_domains = ['startup.registroimprese.it']
    start_urls = ['https://startup.registroimprese.it/isin/home']
    
    # Compile regular expressions once for efficiency
    EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    PHONE_PATTERNS = [
        re.compile(r'\+39\s?\d{10}'),
        re.compile(r'\+39\s?\d{3}[-\s]?\d{7}'),
        re.compile(r'\+39\s?\d{2}[-\s]?\d{8}'),
        re.compile(r'\+39\s?\d{3}[-\s]?\d{3}[-\s]?\d{4}'),
        re.compile(r'\+39\s?\d{3}[-\s]?\d{4}[-\s]?\d{3}'),
        re.compile(r'0\d{1,3}[-\s]?\d{6,7}'),
        re.compile(r'3\d{2}[-\s]?\d{6,7}')
    ]
    DATE_PATTERNS = [
        re.compile(r'(?:costituzione|costituita|foundation|created).*?(\d{2}[/.-]\d{2}[/.-]\d{4})'),
        re.compile(r'(?:costituzione|costituita|foundation|created).*?(\d{4}[/.-]\d{2}[/.-]\d{2})')
    ]
    
    # Define robust selectors based on priority
    COMPANY_NAME_SELECTORS = [
        'h1', 'h2', '.company-name', '#company-name',
        'div.header h1', 'div.intestazione h1',
        '.ragione-sociale', 'title'
    ]
    
    DESCRIPTION_SELECTORS = [
        '.description', '#description',
        '.company-description', '#company-description',
        '.about', '#about', '.profile', '#profile',
        'main p', 'article p', '.content p', '#content p'
    ]
    
    # Labels for various fields (internationalized)
    FIELD_LABELS = {
        'date': ['Data Costituzione', 'Data di costituzione', 'Costituzione', 
                'Foundation Date', 'Data iscrizione', 'Costituita il'],
        'region': ['Regione', 'Region', 'Territory'],
        'city': ['Comune', 'Città', 'City', 'Località', 'Location', 'Sede'],
        'phone': ['Telefono', 'Tel', 'Phone', 'Contatto', 'Contact']
    }
    
    # Better custom settings with explanations
    custom_settings = {
        'DOWNLOAD_DELAY': 2,  # Respectful crawling delay
        'CONCURRENT_REQUESTS': 2,  # Limited concurrency for stability
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'HTTPERROR_ALLOW_ALL': True,
        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'ITEM_PIPELINES': {
            'scrapy.pipelines.files.FilesPipeline': 1,
            '__main__.StartupExportPipeline': 300,
        },
        'FILES_STORE': 'downloads',
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.ImprovedHeadersMiddleware': 550,
        },
        'DUPEFILTER_CLASS': '__main__.CacheURLFilter',
        'LOG_LEVEL': 'INFO',
        # Enable autothrottle for adaptive crawling speed
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 5,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,
    }
    
    def __init__(self, debug=False, *args, **kwargs):
        super(StartupRegistrySpider, self).__init__(*args, **kwargs)
        self.debug = debug
        self.company_count = 0
        
        # Create directories only once
        os.makedirs('downloads', exist_ok=True)
        
        if debug:
            # Only create debug directory if debug mode is enabled
            os.makedirs('debug', exist_ok=True)
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Set up spider with crawler instance and connect signals"""
        spider = super(StartupRegistrySpider, cls).from_crawler(crawler, *args, **kwargs)
        
        # Connect signals for better lifecycle management
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        
        return spider
    
    def spider_opened(self, spider):
        """Runs when the spider starts"""
        logging.info("Spider started")
        self.start_time = time.time()
    
    def spider_closed(self, spider):
        """Runs when the spider finishes"""
        duration = time.time() - self.start_time
        logging.info(f"Spider closed after {duration:.2f} seconds")
        logging.info(f"Processed {self.company_count} companies")
    
    def parse(self, response):
        """Initial parse method for the homepage with smarter form handling"""
        logging.info(f"Parsing homepage: {response.url}")
        
        if self.debug:
            self.save_response(response, 'homepage')
        
        # Look for the main search form
        search_form = None
        
        # First try to find an advanced search form
        for form in response.css('form'):
            action = form.css('::attr(action)').get() or ''
            if 'search' in action or 'ricerca' in action:
                search_form = form
                break
        
        # If no specific search form found, take the first form
        if not search_form and response.css('form'):
            search_form = response.css('form')[0]
        
        # If we found a form, submit it
        if search_form:
            action = search_form.css('::attr(action)').get()
            method = search_form.css('::attr(method)').get() or 'GET'
            
            # Extract form data intelligently
            formdata = {}
            for input_elem in search_form.css('input, select'):
                name = input_elem.css('::attr(name)').get()
                if name:
                    value = input_elem.css('::attr(value)').get() or ''
                    formdata[name] = value
            
            # Add search parameters
            formdata.update({
                'searchType': 'advanced',
                'stato': 'A',  # Active companies
            })
            
            # Build form action URL
            action_url = urljoin(response.url, action) if action else 'https://startup.registroimprese.it/isin/search'
            
            logging.info(f"Submitting form to {action_url}")
            
            # Submit the form with proper method
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
        else:
            # If no form found, try direct URLs
            fallback_urls = [
                'https://startup.registroimprese.it/isin/search?searchType=advanced',
                'https://startup.registroimprese.it/isin/search',
                'https://startup.registroimprese.it/isin/ricerca'
            ]
            
            for url in fallback_urls:
                yield scrapy.Request(
                    url,
                    callback=self.parse_search_results,
                    errback=self.handle_error,
                    meta={'dont_redirect': False, 'handle_httpstatus_list': [302, 301]}
                )
    
    def parse_search_results(self, response):
        """Parse the search results page with better link extraction"""
        logging.info(f"Parsing search results: {response.url}")
        
        if self.debug:
            self.save_response(response, 'search_results')
        
        # Extract company links more efficiently
        company_links = []
        
        # First try with targeted selectors for higher precision
        company_selectors = [
            'a[href*="/company/"]',
            'a[href*="/startup/"]', 
            'a[href*="/detail/"]', 
            'a[href*="/scheda/"]',
            'a[href*="/impresa/"]',
            'a[href*="?id="]'
        ]
        
        # Try each selector in order of specificity
        for selector in company_selectors:
            links = response.css(selector)
            if links:
                for link in links:
                    href = link.css('::attr(href)').get()
                    if href:
                        absolute_url = urljoin(response.url, href)
                        if absolute_url not in company_links:
                            company_links.append(absolute_url)
        
        # If specific selectors didn't work, try table rows
        if not company_links:
            # Look for tables with company data
            table_selectors = ['table.results', 'table.companies', 'table.data', 'table']
            for table_selector in table_selectors:
                rows = response.css(f'{table_selector} tr')
                for row in rows:
                    links = row.css('a[href]')
                    for link in links:
                        href = link.css('::attr(href)').get()
                        if href and not href.startswith('#'):
                            absolute_url = urljoin(response.url, href)
                            if absolute_url not in company_links:
                                company_links.append(absolute_url)
        
        # Process company links with tracking
        for url in company_links:
            # Generate request with proper meta
            yield scrapy.Request(
                url,
                callback=self.parse_company,
                errback=self.handle_error,
                meta={'company_url': url}
            )
        
        # Check for pagination efficiently
        next_page = None
        
        # Try to find next page link with priority selectors
        next_selectors = [
            'a:contains("Next")', 'a:contains("Successivo")',
            'a:contains("Avanti")', 'a:contains("»")',
            'a.next', 'a[rel="next"]',
            'li.next a', 'a[aria-label="Next"]',
            'a[aria-label="Successivo"]'
        ]
        
        for selector in next_selectors:
            next_links = response.css(selector)
            if next_links:
                href = next_links[0].css('::attr(href)').get()
                if href:
                    next_page = urljoin(response.url, href)
                    break
        
        # If explicit next page not found, try to find pagination with current page
        if not next_page:
            # Find active page element
            active_selectors = ['li.active', 'li.selected', 'a.active', 'a.selected', '.pagination .current']
            current_page = None
            
            for selector in active_selectors:
                active = response.css(selector)
                if active:
                    current_text = active.css('::text').get()
                    if current_text and current_text.strip().isdigit():
                        current_page = int(current_text.strip())
                        
                        # Look for link to next page
                        next_page_num = current_page + 1
                        next_page_links = response.css(f'a:contains("{next_page_num}")')
                        if next_page_links:
                            href = next_page_links[0].css('::attr(href)').get()
                            if href:
                                next_page = urljoin(response.url, href)
                                break
        
        # Follow next page if found
        if next_page:
            logging.info(f"Following next page: {next_page}")
            yield scrapy.Request(
                next_page,
                callback=self.parse_search_results,
                errback=self.handle_error
            )
    
    def parse_company(self, response):
        """Extract company information using ItemLoader for clean extraction"""
        company_url = response.meta.get('company_url', response.url)
        logging.info(f"Parsing company page: {company_url}")
        
        # Calculate company content fingerprint for deduplication
        content_hash = hashlib.md5(response.body).hexdigest()
        
        # Register fingerprint with dupefilter
        dupefilter = self.crawler.engine.slot.scheduler.df
        if hasattr(dupefilter, 'register_company_fingerprint'):
            dupefilter.register_company_fingerprint(content_hash)
        
        if self.debug:
            self.save_response(response, f'company_{self.company_count + 1}')
        
        # Use ItemLoader for cleaner data extraction
        loader = StartupLoader(item=StartupItem(), response=response)
        
        # 1. Extract company name with priority selectors
        for selector in self.COMPANY_NAME_SELECTORS:
            loader.add_css('company_name', f'{selector}::text')
        
        # If no name found with CSS, try xpath with text content
        if not loader.get_collected_values('company_name'):
            for selector in self.COMPANY_NAME_SELECTORS:
                loader.add_xpath('company_name', f'//{selector}/text()')
        
        # 2. Extract labeled fields using the helper method
        self.extract_field_with_labels(response, loader, 'creation_date', self.FIELD_LABELS['date'])
        self.extract_field_with_labels(response, loader, 'region', self.FIELD_LABELS['region'])
        self.extract_field_with_labels(response, loader, 'city', self.FIELD_LABELS['city'])
        self.extract_field_with_labels(response, loader, 'phone', self.FIELD_LABELS['phone'])
        
        # 3. Try to find creation date with regex if not found with labels
        if not loader.get_collected_values('creation_date'):
            for pattern in self.DATE_PATTERNS:
                matches = pattern.findall(response.text)
                if matches:
                    loader.add_value('creation_date', matches[0])
                    break
        
        # 4. Extract description with priority selectors
        for selector in self.DESCRIPTION_SELECTORS:
            loader.add_css('description', f'{selector}::text')
        
        # If no description found, try xpath
        if not loader.get_collected_values('description'):
            for selector in self.DESCRIPTION_SELECTORS:
                loader.add_xpath('description', f'//{selector}/text()')
        
        # 5. Extract email
        # First try mailto links
        email_links = response.css('a[href^="mailto:"]::attr(href)').getall()
        if email_links:
            for link in email_links:
                loader.add_value('email', link)
                break
        
        # If not found, try regex on the page content
        if not loader.get_collected_values('email'):
            email_matches = self.EMAIL_PATTERN.findall(response.text)
            # Filter out common non-company emails
            filtered_emails = [e for e in email_matches if not any(d in e.lower() for d in ['example.com', 'gmail.com', 'libero.it', 'hotmail'])]
            if filtered_emails:
                loader.add_value('email', filtered_emails[0])
            elif email_matches:
                loader.add_value('email', email_matches[0])
        
        # 6. Extract phone with regex if not found with labels
        if not loader.get_collected_values('phone'):
            for pattern in self.PHONE_PATTERNS:
                matches = pattern.findall(response.text)
                if matches:
                    loader.add_value('phone', matches[0])
                    break
        
        # 7. Look for downloadable files
        file_urls = []
        
        # More specific file selectors
        file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']
        
        # First look for links with these extensions
        for ext in file_extensions:
            urls = response.css(f'a[href$="{ext}"]::attr(href)').getall()
            for url in urls:
                if url:
                    absolute_url = urljoin(response.url, url)
                    if absolute_url not in file_urls:
                        file_urls.append(absolute_url)
        
        # Then look for download-related links
        download_indicators = ['download', 'allegato', 'attachment', 'file', 'documento']
        for indicator in download_indicators:
            urls = response.css(f'a[href*="{indicator}"]::attr(href)').getall()
            for url in urls:
                if url:
                    absolute_url = urljoin(response.url, url)
                    if absolute_url not in file_urls and not any(absolute_url.endswith(ext) for ext in file_extensions):
                        file_urls.append(absolute_url)
        
        if file_urls:
            loader.add_value('file_urls', file_urls)
        
        # Increment company counter
        self.company_count += 1
        
        # Return the loaded item
        return loader.load_item()
    
    def extract_field_with_labels(self, response, loader, field_name, labels):
        """Extract content associated with a label using more efficient approach"""
        # Try all the label options with different selector strategies
        
        # First look for label-value pairs in structured data
        for label in labels:
            # Method 1: Look for elements containing the label with a colon
            elements = response.xpath(f'//*[contains(text(), "{label}:")]')
            for element in elements:
                full_text = element.css('::text').get() or ''
                if ':' in full_text:
                    parts = full_text.split(':', 1)
                    if len(parts) > 1 and parts[1].strip():
                        loader.add_value(field_name, parts[1].strip())
                        return
            
            # Method 2: Look for elements containing the label followed by a value element
            elements = response.xpath(f'//*[contains(text(), "{label}")]')
            for element in elements:
                # Check next sibling
                next_text = element.xpath('./following-sibling::*[1]//text()').get()
                if next_text and next_text.strip():
                    loader.add_value(field_name, next_text.strip())
                    return
                
                # Check next element in a div or span
                next_in_container = element.xpath('../*[position()=count(../*[.=current()])+1]//text()').get()
                if next_in_container and next_in_container.strip():
                    loader.add_value(field_name, next_in_container.strip())
                    return
        
        # Method 3: Look for table structures with labels
        for label in labels:
            # Find table cells containing the label
            cells = response.xpath(f'//td[contains(text(), "{label}")]')
            for cell in cells:
                # Try to get the next cell in the same row
                next_cell = cell.xpath('./following-sibling::td[1]//text()').get()
                if next_cell and next_cell.strip():
                    loader.add_value(field_name, next_cell.strip())
                    return
        
        # Method 4: Try definition lists
        for label in labels:
            dt = response.css(f'dt:contains("{label}")')
            if dt:
                dd = dt.xpath('./following-sibling::dd[1]//text()').get()
                if dd and dd.strip():
                    loader.add_value(field_name, dd.strip())
                    return
    
    def save_response(self, response, name):
        """Save response for debugging only when in debug mode"""
        if not self.debug:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join('debug', f"{name}_{timestamp}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logging.info(f"Saved debug response to {filename}")
    
    def handle_error(self, failure):
        """Handle request errors with better logging"""
        request = failure.request
        
        if hasattr(failure.value, 'response') and failure.value.response:
            response = failure.value.response
            logging.error(f"Request to {request.url} failed with status {response.status}")
            
            if self.debug:
                self.save_response(response, f"error_{response.status}")
        else:
            logging.error(f"Request to {request.url} failed: {failure.value}")
        
        # Retry with adjusted parameters if needed
        if request.meta.get('retry_count', 0) < 2:
            retry_count = request.meta.get('retry_count', 0) + 1
            logging.info(f"Retrying request to {request.url} (attempt {retry_count})")
            
            # Create new request with increased retry count
            new_request = request.copy()
            new_request.meta['retry_count'] = retry_count
            new_request.dont_filter = True
            
            return new_request
        
        return None


class ImprovedHeadersMiddleware:
    """Enhanced middleware to add realistic browser headers to requests"""
    
    def process_request(self, request, spider):
        # Define a set of realistic headers
        headers = {
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
        }
        
        # Add referrer for better simulation
        if 'startup.registroimprese.it' in request.url:
            headers['Referer'] = 'https://startup.registroimprese.it/isin/home'
        
        # Update request headers
        request.headers.update(headers)
        return None


class StartupExportPipeline:
    """Enhanced pipeline for exporting startup data"""
    
    def __init__(self):
        self.file = None
        self.exporter = None
        self.file_path = 'italian_startups.csv'
    
    def open_spider(self, spider):
        """Initialize the exporter when the spider starts"""
        self.file = open(self.file_path, 'wb')
        self.exporter = CsvItemExporter(
            self.file, 
            fields_to_export=[
                'company_name', 'creation_date', 'region', 
                'city', 'description', 'email', 'phone'
            ],
            encoding='utf-8'
        )
        self.exporter.start_exporting()
    
    def close_spider(self, spider):
        """Clean up when the spider finishes"""
        if self.exporter:
            self.exporter.finish_exporting()
        if self.file:
            self.file.close()
    
    def process_item(self, item, spider):
        """Process and export each item"""
        self.exporter.export_item(item)
        return item


if __name__ == '__main__':
    # Define command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Crawl Italian startup registry')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of companies to crawl (0 for no limit)')
    args = parser.parse_args()
    
    # Configure crawler settings
    settings = get_project_settings()
    settings.update({
        'BOT_NAME': 'startup_registry_crawler',
        'LOG_LEVEL': 'DEBUG' if args.debug else 'INFO',
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'HTTPERROR_ALLOW_ALL': True,
        'ITEM_PIPELINES': {
            'scrapy.pipelines.files.FilesPipeline': 1,
            '__main__.StartupExportPipeline': 300,
        },
        'FILES_STORE': 'downloads',
        'CONCURRENT_REQUESTS': 2,
        'COOKIES_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.ImprovedHeadersMiddleware': 550,
        },
        'DUPEFILTER_CLASS': '__main__.CacheURLFilter',
        'CLOSESPIDER_ITEMCOUNT': args.limit if args.limit > 0 else 0,
        'AUTOTHROTTLE_ENABLED': True,
    })
    
    # Run the crawler
    process = CrawlerProcess(settings)
    process.crawl(StartupRegistrySpider, debug=args.debug)
    process.start() 