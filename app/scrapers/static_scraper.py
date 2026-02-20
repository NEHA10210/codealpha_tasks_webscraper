"""
Static Web Scraper - Handles static websites using requests + BeautifulSoup
"""

import requests
import time
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import html
from datetime import datetime, timezone

from ..utils.logger import setup_logger

class StaticScraper:
    """Robust static web scraper with fallback selectors"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = setup_logger('static_scraper')
        
        # Default configuration
        self.default_config = {
            'delay': 1.0,
            'timeout': 30,
            'max_retries': 3,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
        }
        
        # Merge with user config
        self.config = {**self.default_config, **self.config}
        
        # Initialize session
        self.session = requests.Session()
        self.session.headers.update(self.config['headers'])
        self.session.headers['User-Agent'] = self.config['user_agent']
    
    def scrape(self, url: str) -> Optional[Dict[str, Any]]:
        """Main scraping method"""
        try:
            self.logger.info(f"Starting static scraping of {url}")
            
            # Fetch page
            fetched = self._fetch_page(url)
            if not fetched:
                return None

            soup, fetch_meta = fetched
            
            # Extract data
            result = {
                'url': url,
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'scraping_method': 'static',
                'http_status_code': fetch_meta.get('http_status_code'),
                'response_time_ms': fetch_meta.get('response_time_ms'),
                'user_agent': fetch_meta.get('user_agent'),
                'content_language': fetch_meta.get('content_language'),
                'metadata': self._extract_metadata(soup),
                'content': self._extract_content(soup, url),
                'links': self._extract_links(soup, url),
                'images': self._extract_images(soup, url),
                'forms': self._extract_forms(soup, url)
            }
            
            # Validate result
            if not self._validate_result(result):
                self.logger.warning("Scraped data validation failed")
                return None
            
            self.logger.info(f"Successfully scraped {url}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return None
    
    def _fetch_page(self, url: str) -> Optional[tuple[BeautifulSoup, Dict[str, Any]]]:
        """Fetch and parse web page with retries"""
        for attempt in range(self.config['max_retries']):
            try:
                self.logger.debug(f"Attempt {attempt + 1} to fetch {url}")
                
                # Apply delay
                if attempt > 0:
                    time.sleep(self.config['delay'] * (attempt + 1))
                
                request_start = time.perf_counter()
                response = self.session.get(url, timeout=self.config['timeout'])
                response_time_ms = int((time.perf_counter() - request_start) * 1000)
                response.raise_for_status()
                
                # Handle encoding
                if response.encoding == 'ISO-8859-1':
                    response.encoding = response.apparent_encoding
                
                # Parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check for CAPTCHA indicators
                if self._has_captcha_indicators(soup):
                    self.logger.warning("CAPTCHA indicators detected")
                    return None
                
                fetch_meta = {
                    'http_status_code': response.status_code,
                    'response_time_ms': response_time_ms,
                    'user_agent': self.config.get('user_agent'),
                    'content_language': response.headers.get('Content-Language')
                }

                return soup, fetch_meta
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request error (attempt {attempt + 1}): {str(e)}")
                if attempt == self.config['max_retries'] - 1:
                    raise
                continue
        
        return None
    
    def _has_captcha_indicators(self, soup: BeautifulSoup) -> bool:
        """Check for CAPTCHA indicators in page"""
        captcha_indicators = [
            'captcha', 'recaptcha', 'verify', 'robot', 'human', 'prove'
        ]
        
        # Check in text content
        page_text = soup.get_text().lower()
        for indicator in captcha_indicators:
            if indicator in page_text:
                return True
        
        return False
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract page metadata"""
        metadata = {}
        
        # Title
        title_tag = soup.find('title')
        metadata['title'] = title_tag.get_text().strip() if title_tag else ''
        
        # Meta tags
        meta_tags = ['description', 'keywords', 'author', 'robots', 'viewport']
        for tag in meta_tags:
            meta_element = soup.find('meta', attrs={'name': tag})
            if meta_element:
                metadata[tag] = meta_element.get('content', '').strip()
        
        # Open Graph tags
        og_tags = {}
        for meta in soup.find_all('meta', attrs={'property': True}):
            if meta['property'].startswith('og:'):
                og_tags[meta['property']] = meta.get('content', '')
        metadata['open_graph'] = og_tags
        
        # Language
        html_tag = soup.find('html')
        metadata['language'] = html_tag.get('lang', '') if html_tag else ''
        
        return metadata
    
    def _extract_content(self, soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
        """Extract main content"""
        content = {}
        
        # Find main content area
        main_content = self._find_main_content(soup)
        if main_content:
            # Text content
            content['text'] = self._clean_text(main_content.get_text())
            
            # Headings
            content['headings'] = []
            for h in main_content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                content['headings'].append({
                    'level': int(h.name[1]),
                    'text': h.get_text().strip()
                })
            
            # Paragraphs
            content['paragraphs'] = []
            for p in main_content.find_all('p'):
                text = p.get_text().strip()
                if text and len(text) > 10:
                    content['paragraphs'].append(text)
            
            # Lists
            content['lists'] = []
            for ul in main_content.find_all(['ul', 'ol']):
                items = [li.get_text().strip() for li in ul.find_all('li')]
                content['lists'].append({
                    'type': ul.name,
                    'items': items
                })
        
        return content
    
    def _find_main_content(self, soup: BeautifulSoup) -> Optional:
        """Find main content area"""
        # Try semantic selectors
        semantic_selectors = ['main', 'article', 'section']
        for selector in semantic_selectors:
            element = soup.find(selector)
            if element and self._is_content_heavy(element):
                return element
        
        # Try common content containers
        content_selectors = ['.content', '#content', '.main-content', '.post-content']
        for selector in content_selectors:
            element = soup.select_one(selector)
            if element and self._is_content_heavy(element):
                return element
        
        # Fallback to body
        return soup.find('body')
    
    def _is_content_heavy(self, element) -> bool:
        """Check if element contains substantial content"""
        text = element.get_text()
        return len(text.strip()) > 200
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract all links"""
        links = []
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text().strip()
            
            # Make absolute URL
            absolute_url = urljoin(base_url, href)
            
            link_data = {
                'url': absolute_url,
                'text': text,
                'title': a.get('title', ''),
                'is_external': urlparse(absolute_url).netloc != urlparse(base_url).netloc
            }
            
            links.append(link_data)
        
        return links
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract all images"""
        images = []
        
        for img in soup.find_all('img', src=True):
            img_data = {
                'src': urljoin(base_url, img['src']),
                'alt': img.get('alt', ''),
                'title': img.get('title', '')
            }
            images.append(img_data)
        
        return images
    
    def _extract_forms(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract form information"""
        forms = []
        
        for form in soup.find_all('form'):
            form_data = {
                'action': urljoin(base_url, form.get('action', '')),
                'method': form.get('method', 'get').lower(),
                'fields': []
            }
            
            # Extract form fields
            for field in form.find_all(['input', 'select', 'textarea']):
                field_data = {
                    'name': field.get('name', ''),
                    'type': field.get('type', field.name),
                    'id': field.get('id', ''),
                    'required': field.has_attr('required'),
                    'placeholder': field.get('placeholder', '')
                }
                
                if field.name == 'select':
                    # Extract options
                    options = []
                    for option in field.find_all('option'):
                        options.append({
                            'value': option.get('value', ''),
                            'text': option.get_text().strip(),
                            'selected': option.has_attr('selected')
                        })
                    field_data['options'] = options
                
                form_data['fields'].append(field_data)
            
            forms.append(form_data)
        
        return forms
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ''
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        
        return text.strip()
    
    def _validate_result(self, result: Dict[str, Any]) -> bool:
        """Validate scraping result"""
        # Check if we have basic required fields
        if not result.get('url'):
            return False
        
        # Check if we have some content
        content = result.get('content', {})
        if not content.get('text') and not content.get('headings') and not content.get('paragraphs'):
            return False
        
        return True
