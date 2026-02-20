"""
Robots.txt Checker - Ensures ethical scraping compliance
"""

import urllib.robotparser
import urllib.request
import urllib.error
from urllib.parse import urlparse
import time

from ..utils.logger import setup_logger

class RobotsChecker:
    """Handles robots.txt checking for ethical scraping"""
    
    def __init__(self, user_agent: str = "EthicalWebScraper/1.0"):
        self.user_agent = user_agent
        self.logger = setup_logger('robots_checker')
        self.parsers = {}  # Cache robots.txt parsers
        self.cache_timeout = 3600  # 1 hour cache timeout
        self.default_delay = 1.0
    
    def can_scrape(self, url: str) -> bool:
        """Check if we can scrape the given URL according to robots.txt"""
        try:
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Get or create robots.txt parser
            parser = self._get_parser(domain)
            
            if parser:
                # Check if URL is allowed
                can_fetch = parser.can_fetch(self.user_agent, url)
                
                if can_fetch:
                    self.logger.debug(f"Robots.txt allows scraping: {url}")
                else:
                    self.logger.warning(f"Robots.txt disallows scraping: {url}")
                
                return can_fetch
            else:
                # If no robots.txt found, allow scraping
                self.logger.debug(f"No robots.txt found for {domain}, allowing scraping")
                return True
                
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {str(e)}")
            return True
    
    def get_crawl_delay(self, url: str) -> float:
        """Get the crawl delay specified in robots.txt"""
        try:
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            parser = self._get_parser(domain)
            
            if parser:
                delay = parser.crawl_delay(self.user_agent)
                return delay if delay is not None else self.default_delay
            
            return self.default_delay
            
        except Exception as e:
            self.logger.error(f"Error getting crawl delay for {url}: {str(e)}")
            return self.default_delay
    
    def _get_parser(self, domain: str):
        """Get or create robots.txt parser for domain"""
        try:
            # Check cache first
            if domain in self.parsers:
                cache_entry = self.parsers[domain]
                if time.time() - cache_entry['timestamp'] < self.cache_timeout:
                    return cache_entry['parser']
                else:
                    # Cache expired, remove it
                    del self.parsers[domain]
            
            # Create new parser
            parser = urllib.robotparser.RobotFileParser()
            robots_url = f"{domain}/robots.txt"
            
            try:
                # Fetch robots.txt with an explicit timeout.
                # RobotFileParser.read() uses urllib internally without exposing a timeout, which can hang.
                req = urllib.request.Request(
                    robots_url,
                    headers={'User-Agent': self.user_agent}
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    content = resp.read().decode('utf-8', errors='ignore')

                parser.set_url(robots_url)
                parser.parse(content.splitlines())
                
                # Cache the parser
                self.parsers[domain] = {
                    'parser': parser,
                    'timestamp': time.time()
                }
                
                self.logger.debug(f"Loaded robots.txt for {domain}")
                return parser
                
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    # No robots.txt file
                    self.logger.debug(f"No robots.txt found for {domain}")
                else:
                    self.logger.warning(f"HTTP error loading robots.txt for {domain}: {e.code}")
                return None

            except urllib.error.URLError as e:
                # Network/DNS errors: treat as no robots.txt to avoid blocking the scrape indefinitely
                self.logger.warning(f"URL error loading robots.txt for {domain}: {str(e)}")
                return None
                
            except Exception as e:
                self.logger.error(f"Error loading robots.txt for {domain}: {str(e)}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting parser for {domain}: {str(e)}")
            return None
