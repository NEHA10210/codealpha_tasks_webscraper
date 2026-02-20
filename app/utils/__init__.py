"""
Utility functions for the web scraper
"""

from .validators import validate_url, validate_scraping_request
from .logger import setup_logger

__all__ = ['validate_url', 'validate_scraping_request', 'setup_logger']
