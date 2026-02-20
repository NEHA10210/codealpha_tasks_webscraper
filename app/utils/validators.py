"""
Input validation utilities
"""

import re
from urllib.parse import urlparse
from typing import Dict, Any


def validate_url(url: str) -> bool:
    """Validate URL format"""
    if not url or not isinstance(url, str):
        return False
    
    url = url.strip()
    
    try:
        result = urlparse(url)
        if not all([result.scheme, result.netloc]):
            return False
        
        if result.scheme not in ['http', 'https']:
            return False
        
        # Check for valid domain
        domain = result.netloc
        if not re.match(r'^[a-zA-Z0-9.-]+$', domain):
            return False
        
        return True
        
    except Exception:
        return False


def validate_scraping_request(url: str, scraping_type: str) -> Dict[str, Any]:
    """Validate scraping request parameters"""
    result = {
        'valid': True,
        'message': '',
        'warnings': []
    }
    
    # Validate URL
    if not validate_url(url):
        result['valid'] = False
        result['message'] = 'Invalid URL format'
        return result
    
    # Validate scraping type
    if scraping_type not in ['static', 'dynamic', 'auto']:
        result['valid'] = False
        result['message'] = 'Invalid scraping type. Must be static, dynamic, or auto'
        return result
    
    # Check for suspicious URLs
    if _is_suspicious_url(url):
        result['warnings'].append('URL appears suspicious. Please verify it is legitimate.')
    
    # Check for localhost/private IPs
    if _is_private_url(url):
        result['valid'] = False
        result['message'] = 'Cannot scrape private or localhost URLs'
        return result
    
    return result


def _is_suspicious_url(url: str) -> bool:
    """Check if URL appears suspicious"""
    suspicious_patterns = [
        r'localhost',
        r'127\.0\.0\.1',
        r'192\.168\.',
        r'10\.',
        r'172\.1[6-9]\.',
        r'172\.2[0-9]\.',
        r'172\.3[0-1]\.',
        r'file://',
        r'ftp://',
        r'javascript:',
        r'data:',
        r'mailto:',
        r'tel:'
    ]
    
    for pattern in suspicious_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    
    return False


def _is_private_url(url: str) -> bool:
    """Check if URL points to private network"""
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        
        if not hostname:
            return True
        
        private_patterns = [
            r'^localhost$',
            r'^127\.0\.0\.1$',
            r'^192\.168\.',
            r'^10\.',
            r'^172\.1[6-9]\.',
            r'^172\.2[0-9]\.',
            r'^172\.3[0-1]\.',
            r'^169\.254\.',
            r'^::1$',
            r'^fc00:',
            r'^fe80:'
        ]
        
        for pattern in private_patterns:
            if re.search(pattern, hostname, re.IGNORECASE):
                return True
        
        return False
        
    except Exception:
        return True
