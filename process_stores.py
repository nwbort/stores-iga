import asyncio
import json
import glob
from xml.etree import ElementTree as ET

import aiohttp
from bs4 import BeautifulSoup

SITEMAP_FILES_PATTERN = "iga.com.au-stores-sitemap*.xml.txt"
OUTPUT_FILE = "stores.json"

# It's good practice to set a user-agent to identify your scraper
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- Concurrency Limiter ---
# To avoid overwhelming the server, we limit the number of concurrent requests.
CONCURRENCY_LIMIT = 50
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
# ---------------------------

def parse_sitemaps(file_paths):
    """Parses sitemap files to extract all store URLs."""
    urls = set()
    # The XML parser needs to know the namespace
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    for file_path in file_paths:
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            for url_element in root.findall('ns:url', namespace):
                loc_element = url_element.find('ns:loc', namespace)
                if loc_element is not None and loc_element.text:
                    urls.add(loc_element.text.strip())
        except ET.ParseError as e:
            print(f"Error parsing XML file {file_path}: {e}")
        except FileNotFoundError:
            print(f"Sitemap file not found: {file_path}")
    return list(urls)

async def fetch_store_page(session, url):
    """Fetches the HTML content of a single store page."""
    try:
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.text()
    except aiohttp.ClientError as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_store_details(html_content, url):
    """Parses the HTML of a store page to extract details."""
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    store_data = {'url': url}

    # Helper function to safely get text
    def get_text(element):
        return element.text.strip() if element else None

    # --- Basic Info ---
    store_data['name'] = get_text(soup.find('h1', id='store-name'))
    store_data['address_line_1'] = get_text(soup.find('div', id='store-address-line-1'))
    store_data['address_line_2'] = get_text(soup.find('div', id='store-address-line-2'))
    
    phone_element = soup.find('a', id='phone-no')
    store_data['phone'] = get_text(phone_element) if phone_element else None

    directions_element = soup.find('a', class_='external', href=lambda href: href and 'maps.google.com' in href)
    store_data['directions_url'] = directions_element['href'] if directions_element else None

    # --- Store Hours ---
    hours = {}
    hours_table = soup.find('table', id='store-hours-table')
    if hours_table:
        for row in hours_table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) == 2:
                day = get_text(cols[0])
                time_span = cols[1].find('span', class_='week-hours')
                time = get_text(time_span)
                if day and time:
                    hours[day] = time
    store_data['hours'] = hours

    # --- Products and Services ---
    services = []
    # Using the desktop version to avoid duplicates with the mobile one
    services_container = soup.find('div', class_='store-services-desktop')
    if services_container:
        service_elements = services_container.select('div.service > div')
        for service_div in service_elements:
            service_name = get_text(service_div)
            if service_name:
                services.append(service_name)
    store_data['services'] = sorted(services) # Sort services for consistency

    # A quick validation: if name is missing, data is likely bad
    if not store_data.get('name'):
        print(f"Could not parse store name from {url}. Skipping.")
        return None

    return store_data

async def scrape_and_parse_store(session, url):
    """Coordinates fetching and parsing for a single store."""
    html = await fetch_store_page(session, url)
    if html:
        return parse_store_details(html, url)
    return None

async def scrape_with_semaphore(session, url):
    """A wrapper to limit concurrency using the global semaphore."""
    async with semaphore:
        # This will wait until a "slot" is free in the semaphore before running.
        return await scrape_and_parse_store(session, url)

async def main():
    """Main function to run the scraper."""
    sitemap_files = glob.glob(SITEMAP_FILES_PATTERN)
    if not sitemap_files:
        print(f"No sitemap files found matching pattern: {SITEMAP_FILES_PATTERN}")
        return

    print(f"Found sitemap files: {sitemap_files}")
    store_urls = parse_sitemaps(sitemap_files)
    print(f"Found {len(store_urls)} unique store URLs to scrape.")
    
    if not store_urls:
        return

    all_stores_data = []
    async with aiohttp.ClientSession() as session:
        # Create tasks using the semaphore wrapper to control concurrency
        tasks = [scrape_with_semaphore(session, url) for url in store_urls]
        results = await asyncio.gather(*tasks)
        
        # Filter out failed attempts (which return None)
        all_stores_data = [store for store in results if store]

    print(f"Successfully scraped data for {len(all_stores_data)} stores.")

    if not all_stores_data:
        print("No store data was scraped. Exiting.")
        return
        
    # Sort the final list by store name for consistent output
    all_stores_data.sort(key=lambda x: x.get('name', ''))

    # Save to JSON file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_stores_data, f, indent=2, ensure_ascii=False)
    
    print(f"Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
