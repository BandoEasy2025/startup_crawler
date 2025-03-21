#!/usr/bin/env python3
import requests
import csv
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser

class StartupRegistryCrawler:
    def __init__(self, base_url, output_file, delay_range=(2, 5)):
        self.base_url = base_url
        self.output_file = output_file
        self.delay_range = delay_range
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.robot_parser = RobotFileParser()
        self.robot_parser.set_url(urljoin(base_url, '/robots.txt'))
        try:
            self.robot_parser.read()
        except Exception as e:
            print(f"Error reading robots.txt: {e}")

    def can_fetch(self, url):
        try:
            return self.robot_parser.can_fetch('*', url)
        except Exception:
            return True

    def random_delay(self):
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)

    def get_page(self, url):
        if not self.can_fetch(url):
            print(f"Crawling disallowed for: {url}")
            return None
        
        try:
            self.random_delay()
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Failed to get {url}: Status code {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Request error for {url}: {e}")
            return None

    def parse_company_list_page(self, html):
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        company_links = []

        try:
            # This is a placeholder selector - you'll need to inspect the actual HTML to find the correct selector
            company_elements = soup.select('.company-item a') or soup.select('.startup-list .item a')
            
            for element in company_elements:
                link = element.get('href')
                if link:
                    company_links.append(urljoin(self.base_url, link))
        except Exception as e:
            print(f"Error parsing company list: {e}")
        
        return company_links

    def extract_company_data(self, html, url):
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        company_data = {
            'Company Name': '',
            'Description': '',
            'Website': '',
            'Email': '',
            'Phone Number': '',
            'Region': '',
            'City': '',
            'Date of Establishment': '',
            'URL': url
        }
        
        try:
            # Company Name
            name_element = soup.select_one('.company-name') or soup.select_one('h1.name')
            if name_element:
                company_data['Company Name'] = name_element.text.strip()
                
            # Description
            desc_element = soup.select_one('.company-description') or soup.select_one('.description')
            if desc_element:
                company_data['Description'] = desc_element.text.strip()
                
            # Website - look for links or specific fields
            website_element = soup.select_one('.website a') or soup.select_one('.contact a[href^="http"]')
            if website_element:
                company_data['Website'] = website_element.get('href', '').strip()
                
            # Email - look for email links or text
            email_elements = soup.select('a[href^="mailto:"]')
            if email_elements:
                email = email_elements[0].get('href', '').replace('mailto:', '').strip()
                company_data['Email'] = email
                
            # Phone Number
            phone_element = soup.select_one('.phone') or soup.select_one('.contact .tel')
            if phone_element:
                company_data['Phone Number'] = phone_element.text.strip()
                
            # Region
            region_element = soup.select_one('.region') or soup.select_one('.location .region')
            if region_element:
                company_data['Region'] = region_element.text.strip()
                
            # City
            city_element = soup.select_one('.city') or soup.select_one('.location .city')
            if city_element:
                company_data['City'] = city_element.text.strip()
                
            # Date of Establishment
            date_element = soup.select_one('.establishment-date') or soup.select_one('.founded-date')
            if date_element:
                company_data['Date of Establishment'] = date_element.text.strip()

        except Exception as e:
            print(f"Error extracting company data from {url}: {e}")
            
        return company_data

    def get_next_page_url(self, html):
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Look for a next page link - adjust selector based on actual website
            next_page = soup.select_one('.pagination .next a') or soup.select_one('a.next-page')
            if next_page and next_page.get('href'):
                return urljoin(self.base_url, next_page.get('href'))
        except Exception as e:
            print(f"Error finding next page: {e}")
            
        return None

    def save_to_csv(self, data):
        if not data:
            print("No data to save")
            return
            
        try:
            with open(self.output_file, 'w', newline='', encoding='utf-8') as file:
                fieldnames = [
                    'Company Name', 'Description', 'Website', 'Email', 
                    'Phone Number', 'Region', 'City', 'Date of Establishment', 'URL'
                ]
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                for company in data:
                    writer.writerow(company)
            print(f"Data saved to {self.output_file}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def crawl(self, max_pages=10, companies_per_page=None):
        current_url = self.base_url
        current_page = 1
        all_companies_data = []
        
        while current_url and current_page <= max_pages:
            print(f"Crawling page {current_page}: {current_url}")
            
            html = self.get_page(current_url)
            if not html:
                break
                
            company_links = self.parse_company_list_page(html)
            print(f"Found {len(company_links)} company links on page {current_page}")
            
            # Limit companies per page if specified
            if companies_per_page:
                company_links = company_links[:companies_per_page]
            
            for link in company_links:
                print(f"Processing company: {link}")
                company_html = self.get_page(link)
                company_data = self.extract_company_data(company_html, link)
                if company_data:
                    all_companies_data.append(company_data)
            
            # Get next page URL
            current_url = self.get_next_page_url(html)
            current_page += 1
        
        print(f"Crawling completed. Collected data for {len(all_companies_data)} companies.")
        self.save_to_csv(all_companies_data)


def main():
    base_url = "https://startup.registroimprese.it"
    output_file = "startup_registry_data.csv"
    
    crawler = StartupRegistryCrawler(base_url, output_file)
    # Crawl up to 5 pages, with a maximum of 10 companies per page
    crawler.crawl(max_pages=5, companies_per_page=10)

if __name__ == "__main__":
    main()