"""
Dynamic Web Scraper - Handles JavaScript-rendered websites using Playwright
"""

import asyncio
import re
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import html
from datetime import datetime, timezone

from ..utils.logger import setup_logger

class DynamicScraper:
    """Dynamic web scraper using Playwright with smart waits"""
    
    def __init__(self, page, config: Dict[str, Any] = None):
        self.page = page
        self.config = config or {}
        self.logger = setup_logger('dynamic_scraper')
        
        # Default configuration
        self.default_config = {
            'timeout': 30000,
            'wait_strategy': 'networkidle',
            'max_scroll_attempts': 5,
            'scroll_delay': 1000
        }
        
        # Merge with user config
        self.config = {**self.default_config, **self.config}
    
    async def scrape(self, url: str) -> Optional[Dict[str, Any]]:
        """Main scraping method for dynamic content"""
        try:
            self.logger.info(f"Starting dynamic scraping of {url}")
            
            # Wait for page to be fully loaded
            await self._wait_for_page_load()
            
            # Handle lazy loading
            await self._handle_lazy_loading()
            
            # Wait for dynamic content
            await self._wait_for_dynamic_content()
            
            # Get page content
            content = await self.page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract data
            result = {
                'url': url,
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'scraping_method': 'dynamic',
                'metadata': await self._extract_metadata(),
                'content': await self._extract_content(soup, url),
                'links': await self._extract_links(soup, url),
                'images': await self._extract_images(soup, url),
                'forms': await self._extract_forms(soup, url),
                'javascript_info': await self._extract_javascript_info()
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
    
    async def _wait_for_page_load(self):
        """Wait for page to load based on strategy"""
        strategy = self.config['wait_strategy']
        
        if strategy == 'domcontentloaded':
            await self.page.wait_for_load_state('domcontentloaded')
        elif strategy == 'load':
            await self.page.wait_for_load_state('load')
        elif strategy == 'networkidle':
            await self.page.wait_for_load_state('networkidle')
        
        # Additional wait for JavaScript execution
        await asyncio.sleep(1)
    
    async def _handle_lazy_loading(self):
        """Handle lazy loading and infinite scroll"""
        try:
            max_scrolls = self.config['max_scroll_attempts']
            scroll_delay = self.config['scroll_delay']
            
            for _ in range(max_scrolls):
                # Scroll to bottom
                await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                # Wait for content to load
                await asyncio.sleep(scroll_delay / 1000)
                
                # Check if new content loaded
                new_height = await self.page.evaluate('document.body.scrollHeight')
                old_height = await self.page.evaluate('window.innerHeight')
                
                if new_height <= old_height:
                    break
        
        except Exception as e:
            self.logger.warning(f"Error handling lazy loading: {str(e)}")
    
    async def _wait_for_dynamic_content(self):
        """Wait for dynamic content to load"""
        try:
            # Wait for common dynamic content indicators
            dynamic_selectors = [
                '[data-loaded]',
                '.loaded',
                '[data-state="loaded"]'
            ]
            
            for selector in dynamic_selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=5000)
                    self.logger.debug(f"Dynamic content found: {selector}")
                    break
                except:
                    continue
            
            # Wait for AJAX requests to complete
            await self.page.wait_for_load_state('networkidle', timeout=10000)
            
            # Additional wait for animations
            await asyncio.sleep(2)
            
        except Exception as e:
            self.logger.debug(f"Error waiting for dynamic content: {str(e)}")
    
    async def _extract_metadata(self) -> Dict[str, Any]:
        """Extract page metadata using JavaScript"""
        metadata = {}
        
        # Title
        title = await self.page.evaluate('() => document.title')
        metadata['title'] = title.strip() if title else ''
        
        # Meta tags
        meta_tags = await self.page.evaluate('''
            () => {
                const metas = {};
                const metaElements = document.querySelectorAll('meta');
                metaElements.forEach(meta => {
                    const name = meta.name || meta.property || meta.getAttribute('http-equiv');
                    const content = meta.content;
                    if (name && content) {
                        metas[name] = content;
                    }
                });
                return metas;
            }
        ''')
        
        # Extract common meta fields
        for field in ['description', 'keywords', 'author', 'robots', 'viewport']:
            if field in meta_tags:
                metadata[field] = meta_tags[field]
        
        # Open Graph tags
        og_tags = {k: v for k, v in meta_tags.items() if k.startswith('og:')}
        metadata['open_graph'] = og_tags
        
        # URL and domain
        metadata['url'] = await self.page.evaluate('() => window.location.href')
        metadata['domain'] = urlparse(metadata['url']).netloc
        
        # Language
        metadata['language'] = await self.page.evaluate('() => document.documentElement.lang')
        
        return metadata
    
    async def _extract_content(self, soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
        """Extract main content"""
        content = {}
        
        # Find main content area
        main_content = await self._find_main_content()
        if main_content:
            # Get text content
            text_content = await main_content.evaluate('el => el.innerText')
            content['text'] = self._clean_text(text_content)
            
            # Extract structured content
            content['headings'] = await self._extract_headings(main_content)
            content['paragraphs'] = await self._extract_paragraphs(main_content)
            content['lists'] = await self._extract_lists(main_content)
        
        return content
    
    async def _find_main_content(self):
        """Find main content area"""
        # Try semantic selectors first
        semantic_selectors = ['main', 'article', 'section[role="main"]']
        
        for selector in semantic_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    # Check if it has substantial content
                    text_length = await element.evaluate('el => el.innerText.length')
                    if text_length > 200:
                        return element
            except:
                continue
        
        # Try common content containers
        content_selectors = ['.content', '#content', '.main-content']
        
        for selector in content_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    text_length = await element.evaluate('el => el.innerText.length')
                    if text_length > 200:
                        return element
            except:
                continue
        
        # Fallback to body
        return await self.page.query_selector('body')
    
    async def _extract_headings(self, container) -> List[Dict[str, Any]]:
        """Extract headings from container"""
        headings = []
        
        for level in range(1, 7):
            elements = await container.query_selector_all(f'h{level}')
            
            for element in elements:
                heading_data = await element.evaluate('el => ({\n                    text: el.innerText.trim(),\n                    id: el.id || "",\n                    className: el.className || ""\n                })')
                
                headings.append({
                    'level': level,
                    'text': heading_data['text'],
                    'id': heading_data['id'],
                    'class': heading_data['className'].split() if heading_data['className'] else []
                })
        
        return headings
    
    async def _extract_paragraphs(self, container) -> List[str]:
        """Extract paragraphs from container"""
        paragraphs = []
        
        elements = await container.query_selector_all('p')
        
        for element in elements:
            text = await element.evaluate('el => el.innerText.trim()')
            if text and len(text) > 10:
                paragraphs.append(text)
        
        return paragraphs
    
    async def _extract_lists(self, container) -> List[Dict[str, Any]]:
        """Extract lists from container"""
        lists = []
        
        for list_type in ['ul', 'ol']:
            elements = await container.query_selector_all(list_type)
            
            for element in elements:
                items = await element.evaluate("""el => {
                    const items = [];
                    el.querySelectorAll('li').forEach(li => {
                        items.push(li.innerText.trim());
                    });
                    return items;
                }""")
                
                lists.append({
                    'type': list_type,
                    'items': items
                })
        
        return lists
    
    async def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract links from page"""
        links = []
        
        elements = await self.page.query_selector_all('a[href]')
        
        for element in elements:
            link_data = await element.evaluate("""el => ({
                href: el.href,
                text: el.innerText.trim(),
                title: el.title || "",
                target: el.target || ""
            })""")
            
            # Make absolute URL
            absolute_url = urljoin(base_url, link_data['href'])
            
            links.append({
                'url': absolute_url,
                'text': link_data['text'],
                'title': link_data['title'],
                'target': link_data['target'],
                'is_external': urlparse(absolute_url).netloc != urlparse(base_url).netloc
            })
        
        return links
    
    async def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract images from page"""
        images = []
        
        image_data = await self.page.evaluate('''
            () => {
                const images = [];
                document.querySelectorAll('img[src]').forEach(img => {
                    images.push({
                        src: img.src,
                        alt: img.alt || '',
                        title: img.title || '',
                        className: img.className || ''
                    });
                });
                return images;
            }
        ''')
        
        for img in image_data:
            images.append({
                'src': urljoin(base_url, img['src']),
                'alt': img['alt'],
                'title': img['title']
            })
        
        return images
    
    async def _extract_forms(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Extract form information"""
        forms = []
        
        form_elements = await self.page.query_selector_all('form')
        
        for form in form_elements:
            form_data = await form.evaluate("""el => {
                const data = {
                    action: el.action || "",
                    method: el.method || "get",
                    id: el.id || "",
                    className: el.className || "",
                    fields: []
                };
                
                el.querySelectorAll("input, select, textarea").forEach(field => {
                    const fieldData = {
                        name: field.name || "",
                        type: field.type || field.tagName.toLowerCase(),
                        id: field.id || "",
                        className: field.className || "",
                        required: field.hasAttribute("required"),
                        placeholder: field.placeholder || ""
                    };
                    
                    if (field.tagName.toLowerCase() === "select") {
                        fieldData.options = [];
                        field.querySelectorAll("option").forEach(option => {
                            fieldData.options.push({
                                value: option.value || "",
                                text: option.innerText.trim(),
                                selected: option.selected
                            });
                        });
                    }
                    
                    data.fields.push(fieldData);
                });
                
                return data;
            }""")
        
            # Make action URL absolute
            if form_data['action']:
                form_data['action'] = urljoin(base_url, form_data['action'])
            
            forms.append(form_data)
        
        return forms
    
    async def _extract_javascript_info(self) -> Dict[str, Any]:
        """Extract JavaScript-related information"""
        js_info = {}
        
        # Check for common JavaScript frameworks
        frameworks = await self.page.evaluate('''
            () => {
                const frameworks = {};
                
                if (typeof React !== 'undefined' || document.querySelector('[data-reactroot]')) {
                    frameworks.react = true;
                }
                
                if (typeof Vue !== 'undefined' || document.querySelector('[data-v-]')) {
                    frameworks.vue = true;
                }
                
                if (typeof angular !== 'undefined' || document.querySelector('[ng-app]')) {
                    frameworks.angular = true;
                }
                
                if (typeof $ !== 'undefined') {
                    frameworks.jquery = true;
                }
                
                return frameworks;
            }
        ''')
        
        js_info['frameworks'] = frameworks
        
        return js_info
    
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
