#!/bin/bash
set -e

echo "Downloading sitemaps..."
./download.sh 'https://www.iga.com.au/stores-sitemap1.xml'
./download.sh 'https://www.iga.com.au/stores-sitemap2.xml'

echo "Running Python scraper to process stores..."
python3 process_stores.py

echo "Scraping complete."
