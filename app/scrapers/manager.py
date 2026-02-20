"""
Scraping Manager - Orchestrates web scraping operations
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from urllib.parse import urlparse

from .static_scraper import StaticScraper
from .dynamic_scraper import DynamicScraper
from .robots_checker import RobotsChecker
from .captcha_detector import CaptchaDetector
from .data_processor import DataProcessor
from ..utils.logger import setup_logger

class ScrapingManager:
    """Manages web scraping operations with ethical compliance"""
    
    def __init__(self):
        self.logger = setup_logger('scraping_manager')
        self.robots_checker = RobotsChecker()
        self.captcha_detector = CaptchaDetector()
        self.data_processor = DataProcessor()
        self.active_scrapers = {}
    
    def scrape_website(self, url: str, scraping_type: str, config: Dict[str, Any], 
                     session_id: str, progress_callback: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """
        Main scraping method
        """
        try:
            self.logger.info(f"Starting scraping session {session_id} for {url}")
            
            # Update progress
            if progress_callback:
                progress_callback(10, "Checking robots.txt")
            
            # Check robots.txt compliance
            if not self.robots_checker.can_scrape(url):
                self.logger.warning(f"Robots.txt disallows scraping: {url}")
                return self._create_error_result(url, "Robots.txt disallows scraping")
            
            # Update progress
            if progress_callback:
                progress_callback(20, "Checking for CAPTCHA")
            
            # Check for CAPTCHA
            if self.captcha_detector.has_captcha(url):
                self.logger.warning(f"CAPTCHA detected for {url}")
                return self._create_error_result(url, "CAPTCHA detected - scraping blocked")
            
            # Update progress
            if progress_callback:
                progress_callback(30, f"Starting {scraping_type} scraping")
            
            # Perform scraping based on type
            if scraping_type == 'static':
                result = self._scrape_static(url, config, progress_callback)
            elif scraping_type == 'dynamic':
                result = self._scrape_dynamic(url, config, progress_callback)
            else:  # auto
                result = self._scrape_auto(url, config, progress_callback)
            
            if result:
                # Update progress
                if progress_callback:
                    progress_callback(80, "Processing data")
                
                # Process and validate data
                processed_result = self.data_processor.process(result, session_id=session_id)
                
                # Update progress
                if progress_callback:
                    progress_callback(100, "Completed")
                
                self.logger.info(f"Successfully scraped {url}")
                return processed_result
            else:
                return self._create_error_result(url, "Scraping failed - no data retrieved")
                
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return self._create_error_result(url, f"Scraping failed: {str(e)}")
    
    def _scrape_static(self, url: str, config: Dict[str, Any], 
                      progress_callback: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """Scrape static website"""
        try:
            scraper = StaticScraper(config)
            
            # Apply rate limiting
            delay = config.get('delay', 1.0)
            time.sleep(delay)
            
            if progress_callback:
                progress_callback(40, "Fetching static content")
            
            result = scraper.scrape(url)
            
            if progress_callback:
                progress_callback(70, "Extracting static data")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Static scraping error: {str(e)}")
            return None
    
    def _scrape_dynamic(self, url: str, config: Dict[str, Any], 
                       progress_callback: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """Scrape dynamic website"""
        try:
            # Run async Playwright in thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                if progress_callback:
                    progress_callback(40, "Launching browser")
                
                result = loop.run_until_complete(
                    self._scrape_dynamic_async(url, config, progress_callback)
                )
                
                if progress_callback:
                    progress_callback(70, "Extracting dynamic data")
                
                return result
            finally:
                loop.close()
                
        except Exception as e:
            self.logger.error(f"Dynamic scraping error: {str(e)}")
            return None
    
    async def _scrape_dynamic_async(self, url: str, config: Dict[str, Any], 
                                  progress_callback: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """Async Playwright scraping"""
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = None
            page = None
            
            try:
                if progress_callback:
                    progress_callback(50, "Loading page")
                
                # Launch browser
                browser = await p.chromium.launch(
                    headless=config.get('headless', True),
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                
                context = await browser.new_context(
                    user_agent=config.get('user_agent', 
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                )
                
                page = await context.new_page()
                
                # Navigate to page
                goto_start = time.perf_counter()
                response = await page.goto(url, wait_until='networkidle', timeout=config.get('timeout', 30000))
                response_time_ms = int((time.perf_counter() - goto_start) * 1000)

                http_status_code = None
                content_language = None
                if response is not None:
                    try:
                        http_status_code = response.status
                        headers = response.headers
                        content_language = headers.get('content-language')
                    except Exception:
                        pass
                
                # Wait for content
                wait_strategy = config.get('wait_strategy', 'networkidle')
                if wait_strategy == 'domcontentloaded':
                    await page.wait_for_load_state('domcontentloaded')
                elif wait_strategy == 'load':
                    await page.wait_for_load_state('load')
                
                if progress_callback:
                    progress_callback(60, "Waiting for dynamic content")
                
                # Check for CAPTCHA after page load
                if await self._check_captcha_playwright(page):
                    return None
                
                # Handle lazy loading
                await self._handle_lazy_loading(page, config)
                
                # Extract content
                scraper = DynamicScraper(page, config)
                result = await scraper.scrape(url)

                if isinstance(result, dict):
                    result['http_status_code'] = http_status_code
                    result['response_time_ms'] = response_time_ms
                    result['user_agent'] = config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                    result['content_language'] = content_language
                
                return result
                
            except Exception as e:
                self.logger.error(f"Playwright error: {str(e)}")
                raise
            finally:
                # Clean up
                if page:
                    await page.close()
                if browser:
                    await browser.close()
    
    def _scrape_auto(self, url: str, config: Dict[str, Any], 
                    progress_callback: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """Auto-detect and use appropriate scraper"""
        try:
            if progress_callback:
                progress_callback(35, "Analyzing website type")
            
            # Check if site likely uses JavaScript
            is_dynamic = self._detect_dynamic_site(url)
            
            if is_dynamic:
                if progress_callback:
                    progress_callback(40, "Detected dynamic site")
                return self._scrape_dynamic(url, config, progress_callback)
            else:
                if progress_callback:
                    progress_callback(40, "Detected static site")
                return self._scrape_static(url, config, progress_callback)
                
        except Exception as e:
            self.logger.error(f"Auto-detection error: {str(e)}")
            # Fallback to static scraping
            return self._scrape_static(url, config, progress_callback)
    
    def _detect_dynamic_site(self, url: str) -> bool:
        """Detect if website likely uses JavaScript"""
        try:
            import requests
            from bs4 import BeautifulSoup
            
            # Quick check with requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return True
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check for JavaScript frameworks
            js_indicators = [
                'react', 'vue', 'angular', 'ember', 'backbone',
                'webpack', 'parcel', 'rollup', 'vite'
            ]
            
            page_html = str(soup).lower()
            
            for indicator in js_indicators:
                if indicator in page_html:
                    return True
            
            # Check for spa indicators
            spa_indicators = [
                'data-react', 'ng-app', 'v-app', '#app', '.app',
                'router-view', 'component', 'directive'
            ]
            
            for indicator in spa_indicators:
                if indicator in page_html:
                    return True
            
            # Check for minimal content
            text_content = soup.get_text(strip=True)
            if len(text_content) < 100:
                return True
            
            return False
            
        except Exception:
            return True
    
    async def _check_captcha_playwright(self, page) -> bool:
        """Check for CAPTCHA using Playwright"""
        captcha_selectors = [
            'iframe[src*="recaptcha"]',
            'iframe[src*="captcha"]',
            '.g-recaptcha',
            '#captcha',
            '[class*="captcha"]',
            '[id*="captcha"]'
        ]
        
        for selector in captcha_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    return True
            except:
                continue
        
        # Check for CAPTCHA text
        page_text = await page.evaluate('() => document.body.innerText')
        captcha_keywords = ['captcha', 'verify', 'robot', 'human', 'prove']
        
        for keyword in captcha_keywords:
            if keyword.lower() in page_text.lower():
                return True
        
        return False
    
    async def _handle_lazy_loading(self, page, config: Dict[str, Any]):
        """Handle lazy loading and infinite scroll"""
        try:
            max_scrolls = config.get('max_scroll_attempts', 5)
            scroll_delay = config.get('scroll_delay', 1000)
            
            for _ in range(max_scrolls):
                # Scroll to bottom
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                # Wait for content to load
                await asyncio.sleep(scroll_delay / 1000)
                
                # Check if new content loaded
                new_height = await page.evaluate('document.body.scrollHeight')
                old_height = await page.evaluate('window.innerHeight')
                
                if new_height <= old_height:
                    break
        
        except Exception as e:
            self.logger.warning(f"Error handling lazy loading: {str(e)}")
    
    def _create_error_result(self, url: str, error_message: str) -> Dict[str, Any]:
        """Create error result"""
        return {
            'url': url,
            'status': 'error',
            'error': error_message,
            'scraped_at': datetime.now().isoformat(),
            'data': None
        }
    
    def stop_scraping(self, session_id: str):
        """Stop active scraping session"""
        if session_id in self.active_scrapers:
            scraper = self.active_scrapers[session_id]
            # Implementation would depend on scraper type
            self.logger.info(f"Stopping scraping session {session_id}")
            del self.active_scrapers[session_id]
