"""
CAPTCHA Detector - Detects CAPTCHAs and anti-bot measures
"""

import re
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

from ..utils.logger import setup_logger

class CaptchaDetector:
    """Detects CAPTCHAs and anti-bot measures"""
    
    def __init__(self):
        self.logger = setup_logger('captcha_detector')
        
        # CAPTCHA indicators
        self.captcha_selectors = [
            'iframe[src*="recaptcha"]',
            'iframe[src*="hcaptcha"]',
            '.g-recaptcha',
            '[data-sitekey]',
            '#captcha',
            '.captcha',
            '[class*="captcha"]',
            '[id*="captcha"]'
        ]
        
        # CAPTCHA text indicators
        self.captcha_keywords = [
            'captcha', 'recaptcha', 'hcaptcha',
            'verify', 'verification', 'prove', 'robot', 'human'
        ]
    
    def has_captcha(self, url: str) -> bool:
        """Check if a URL has CAPTCHA or anti-bot measures"""
        try:
            # Check with requests
            has_captcha = self._check_with_requests(url)
            
            if has_captcha:
                self.logger.info(f"CAPTCHA detected for {url}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking CAPTCHA for {url}: {str(e)}")
            return False
    
    def _check_with_requests(self, url: str) -> bool:
        """Check for CAPTCHA using HTTP requests"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
            
            # Check response headers for anti-bot indicators
            if self._check_response_headers(response.headers):
                return True
            
            # Check response content
            if response.status_code == 200:
                detection_result = self._detect_captcha_in_content(response.text)
                return detection_result['has_captcha']
            
            # Check for specific status codes
            if response.status_code in [403, 429, 503]:
                self.logger.warning(f"Suspicious status code {response.status_code} for {url}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error in requests check: {str(e)}")
            return False
    
    def _detect_captcha_in_content(self, content: str) -> Dict[str, Any]:
        """Detect CAPTCHA in HTML content"""
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Check for CAPTCHA selectors
            for selector in self.captcha_selectors:
                try:
                    elements = soup.select(selector)
                    if elements:
                        return {'has_captcha': True, 'type': 'selector', 'indicator': selector}
                except:
                    continue
            
            # Check for CAPTCHA text
            page_text = soup.get_text().lower()
            for keyword in self.captcha_keywords:
                if keyword in page_text:
                    return {'has_captcha': True, 'type': 'text', 'indicator': keyword}
            
            return {'has_captcha': False}
            
        except Exception as e:
            self.logger.error(f"Error detecting CAPTCHA in content: {str(e)}")
            return {'has_captcha': False}
    
    def _check_response_headers(self, headers) -> bool:
        """Check response headers for anti-bot indicators"""
        try:
            # Check for Cloudflare headers
            if 'server' in headers and 'cloudflare' in headers['server'].lower():
                self.logger.info("Cloudflare protection detected")
                return True
            
            return False
            
        except Exception:
            return False
