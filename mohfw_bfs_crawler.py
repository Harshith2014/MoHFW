import requests
from bs4 import BeautifulSoup
import urllib.parse
from collections import deque
import os
import json
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)

class MoHFWCrawler:
    def __init__(self):
        self.base_url = "https://mohfw.gov.in"
        self.seed_url = "https://mohfw.gov.in"
        self.download_dir = os.path.join("data_dump", "General_Medicine", "MoHFW")
        self.visited = set()
        self.queue = deque([self.seed_url])
        self.pdf_min_size = 50 * 1024  # 50KB
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Statistics
        self.pages_visited = 0
        self.pdfs_downloaded = 0
        self.total_size_bytes = 0

        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)

    def is_valid_url(self, url):
        """Check if URL belongs to domain and hasn't been visited."""
        parsed = urllib.parse.urlparse(url)
        return (
            parsed.netloc.endswith("mohfw.gov.in") and
            url not in self.visited
        )

    def get_response(self, url, stream=False):
        """Robust request with retries."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, stream=stream, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                time.sleep(2)  # Backoff
        logging.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None

    def save_pdf(self, url, response):
        """Save PDF and create metadata sidecar."""
        try:
            content_length = int(response.headers.get('content-length', 0))
            if content_length < self.pdf_min_size:
                logging.info(f"Skipping small PDF ({content_length} bytes): {url}")
                return

            filename = os.path.basename(urllib.parse.urlparse(url).path)
            # Handle empty filenames or query params
            if not filename or filename.endswith('/'):
                filename = f"document_{int(time.time())}.pdf"
            if not filename.lower().endswith('.pdf'):
                filename += ".pdf"
            
            # Clean filename
            filename = "".join(c for c in filename if c.isalnum() or c in (' ', '.', '_', '-'))
            
            file_path = os.path.join(self.download_dir, filename)
            
            # Save PDF
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.pdfs_downloaded += 1
            self.total_size_bytes += content_length
            
            # Create Metadata
            metadata = {
                "source_authority": "MoHFW",
                "tier": "Tier1",
                "download_url": url,
                "file_name": filename,
                "crawl_date": time.strftime("%Y-%m-%d"),
                "file_size_kb": round(content_length / 1024, 2)
            }
            
            json_path = file_path + ".json"
            with open(json_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logging.info(f"Downloaded: {filename} ({metadata['file_size_kb']} KB)")
            
        except Exception as e:
            logging.error(f"Error saving PDF {url}: {e}")

    def crawl(self):
        logging.info("Starting crawl...")
        
        while self.queue:
            url = self.queue.popleft()
            
            if url in self.visited:
                continue
            
            logging.info(f"Visiting: {url}")
            self.visited.add(url)
            self.pages_visited += 1
            
            # Polite delay
            time.sleep(1)
            
            # Check if it's a direct PDF link before requesting content type if possible, 
            # but usually we need to GET to know. For BFS, we normally GET pages.
            # If the URL itself ends in PDF, we can treat it as a resource.
            # However, the requirement says "Crawl only inside... Download only links ending in .pdf"
            # So we usually find PDF links on HTML pages.
            # But if a Redirect leads to a PDF, we might handle it.
            
            try:
                # We do a HEAD request first to check content type if unsure, 
                # but typically we just GET text/html.
                # If we encounter a PDF link in the Wild, we download it. 
                # The queue might contain PDF links if we added them.
                # Let's assume queue contains Pages to scan, and we verify links ON the page.
                
                # Correction: BFS means we add ALL valid links to queue.
                # If we pop a PDF url, we download it. If HTML, we parse it.
                
                response = self.get_response(url, stream=True) # Stream to check headers first
                if not response:
                    continue

                content_type = response.headers.get('Content-Type', '').lower()
                
                if 'application/pdf' in content_type or url.lower().endswith('.pdf'):
                    self.save_pdf(url, response)
                elif 'text/html' in content_type:
                    # It's a page, consume it to find more links
                    # We need full content for soup
                    # Close the stream response and get new one or read content?
                    # response.content will read it.
                    try:
                        html_content = response.content # read into memory
                    except Exception:
                        continue 

                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        full_url = urllib.parse.urljoin(url, href)
                        
                        # Remove fragment
                        full_url = urllib.parse.urlparse(full_url)._replace(fragment="").geturl()
                        
                        if self.is_valid_url(full_url):
                             if full_url not in self.visited and full_url not in self.queue:
                                self.queue.append(full_url)
                else:
                    logging.info(f"Skipping content type: {content_type}")
                    
            except Exception as e:
                logging.error(f"Error processing {url}: {e}")
                
            # Progress Report
            print(f"Progress: Pages Visited: {self.pages_visited} | PDFs: {self.pdfs_downloaded} | Total Size: {self.total_size_bytes/1024/1024:.2f} MB", end='\r')

        logging.info("\nCrawl Complete.")
        logging.info(f"Total Pages Visited: {self.pages_visited}")
        logging.info(f"Total PDFs Downloaded: {self.pdfs_downloaded}")

if __name__ == "__main__":
    crawler = MoHFWCrawler()
    crawler.crawl()
