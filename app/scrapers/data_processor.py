"""
Data Processor - Validates and cleans scraped data
"""

import re
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin
from urllib.parse import urlparse
from datetime import datetime, timezone
import uuid

from ..utils.logger import setup_logger

class DataProcessor:
    """Validates and cleans scraped data"""
    
    def __init__(self):
        self.logger = setup_logger('data_processor')
    
    def process(self, raw_data: Dict[str, Any], session_id: Optional[str] = None) -> Dict[str, Any]:
        """Process and validate scraped data"""
        try:
            # Validate raw data
            validation_result = self._validate_raw_data(raw_data)
            
            if not validation_result['valid']:
                self.logger.warning("Raw data validation failed")
                return self._create_error_result(raw_data.get('url', ''), validation_result['errors'])
            
            # Clean and normalize data
            cleaned_data = self._clean_data(raw_data)
            
            # Remove duplicates
            deduplicated_data = self._remove_duplicates(cleaned_data)
            
            # Normalize URLs
            normalized_data = self._normalize_urls(deduplicated_data)
            
            # Always generate a unique UUID for page_id to ensure uniqueness
            page_id = session_id or str(uuid.uuid4())
            
            # Validate page_id exists
            if not page_id:
                page_id = str(uuid.uuid4())

            normalized_tables = self._to_normalized_tables(normalized_data, page_id)

            # Create final result
            result = {
                'page_id': page_id,
                'url': raw_data.get('url', ''),
                'status': 'success',
                'scraped_at': raw_data.get('scraped_at') or datetime.now(timezone.utc).isoformat(),
                'scrape_method': raw_data.get('scraping_method', raw_data.get('scrape_method', 'unknown')),
                'metadata': raw_data.get('metadata', {}),
                'tables': normalized_tables,
                'statistics': self._generate_statistics(normalized_data),
                'data': {
                    'pages_scraped': len(normalized_tables.get('pages', [])),
                    'links_found': len(normalized_tables.get('links', [])),
                    'images_found': len(normalized_data.get('images', [])),
                    'headings_found': len(normalized_data.get('content', {}).get('headings', [])),
                    'paragraphs_found': len(normalized_data.get('content', {}).get('paragraphs', [])),
                    'forms_found': len(normalized_data.get('forms', [])),
                    'word_count': len(normalized_data.get('content', {}).get('text', '').split()) if normalized_data.get('content', {}).get('text') else 0,
                    'character_count': len(normalized_data.get('content', {}).get('text', ''))
                },
                'validation': validation_result
            }
            
            self.logger.info(f"Successfully processed data for {raw_data.get('url', '')}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return self._create_error_result(raw_data.get('url', ''), f"Processing error: {str(e)}")
    
    def _validate_raw_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate raw scraped data"""
        result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        if not data.get('url'):
            result['valid'] = False
            result['errors'].append('Missing URL')
        
        # Check content
        content = data.get('content', {})
        if not content.get('text') and not content.get('headings') and not content.get('paragraphs'):
            result['warnings'].append('No content extracted')
        
        # Check metadata
        metadata = data.get('metadata', {})
        if not metadata.get('title'):
            result['warnings'].append('No title found')
        
        return result
    
    def _clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize scraped data"""
        cleaned = data.copy()
        
        # Clean text content
        if 'content' in cleaned:
            content = cleaned['content']
            if 'text' in content:
                content['text'] = self._clean_text(content['text'])
            
            if 'paragraphs' in content:
                content['paragraphs'] = [self._clean_text(p) for p in content['paragraphs'] if p.strip()]
            
            if 'headings' in content:
                for heading in content['headings']:
                    heading['text'] = self._clean_text(heading['text'])
        
        # Clean metadata
        if 'metadata' in cleaned:
            metadata = cleaned['metadata']
            for key in metadata:
                if isinstance(metadata[key], str):
                    metadata[key] = self._clean_text(metadata[key])
        
        return cleaned
    
    def _clean_text(self, text: str) -> str:
        """Clean text content"""
        if not isinstance(text, str):
            return str(text)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        
        return text.strip()
    
    def _remove_duplicates(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove duplicate data"""
        if 'links' in data and isinstance(data['links'], list):
            seen_urls = set()
            unique_links = []
            
            for link in data['links']:
                if isinstance(link, dict) and 'url' in link:
                    url = link['url']
                    if url not in seen_urls:
                        seen_urls.add(url)
                        unique_links.append(link)
            
            data['links'] = unique_links
        
        if 'images' in data and isinstance(data['images'], list):
            seen_srcs = set()
            unique_images = []
            
            for image in data['images']:
                if isinstance(image, dict) and 'src' in image:
                    src = image['src']
                    if src not in seen_srcs:
                        seen_srcs.add(src)
                        unique_images.append(image)
            
            data['images'] = unique_images
        
        return data

    def _is_valid_url(self, url: str) -> bool:
        try:
            if not isinstance(url, str):
                return False

            url = url.strip()
            if not url:
                return False

            lowered = url.lower()
            # Filter out non-navigational / non-http(s) URLs commonly found in href
            if lowered.startswith('#'):
                return False
            if lowered.startswith(('javascript:', 'mailto:', 'tel:', 'data:', 'ftp:', 'file:')):
                return False
            # Reject obvious bad values (anchor text accidentally stored as url)
            if any(ch.isspace() for ch in url):
                return False

            parsed = urlparse(url)
            if parsed.scheme not in ['http', 'https']:
                return False
            if not parsed.netloc:
                return False
            return True
        except Exception:
            return False

    def _is_external_link(self, link_url: str, base_url: str) -> bool:
        """Determine if a link is external by comparing domains"""
        try:
            link_parsed = urlparse(link_url)
            base_parsed = urlparse(base_url)
            
            # Direct netloc comparison as specified
            return link_parsed.netloc != base_parsed.netloc
        except Exception:
            # If we can't parse, assume external to be safe
            return True

    def _to_normalized_tables(self, data: Dict[str, Any], page_id: str) -> Dict[str, List[Dict[str, Any]]]:
        # Ensure full ISO format timestamp with UTC
        scraped_at = data.get('scraped_at')
        if scraped_at:
            # Convert to full ISO format if needed
            if len(scraped_at) < 20:  # If it's just date or short format
                scraped_at_utc = datetime.now(timezone.utc).isoformat()
            else:
                scraped_at_utc = scraped_at
        else:
            scraped_at_utc = datetime.now(timezone.utc).isoformat()

        metadata = data.get('metadata') or {}
        content = data.get('content') or {}
        text_content = content.get('text') or ''
        base_url = data.get('url', '')

        page_row = {
            'page_id': page_id,
            'scraped_at': scraped_at_utc,
            'url': base_url,
            'title': metadata.get('title', ''),
            'text_length': len(text_content),
            'paragraph_count': len(content.get('paragraphs', []) or []),
            'heading_count': len(content.get('headings', []) or []),
            'image_count': len(data.get('images', []) or []),
            'form_count': len(data.get('forms', []) or []),
            # Add HTTP status tracking
            'http_status_code': data.get('http_status_code', 200),
            'response_time_ms': data.get('response_time_ms', 0),
        }

        links_rows: List[Dict[str, Any]] = []
        link_number = 0
        for link in data.get('links', []) or []:
            if not isinstance(link, dict):
                continue

            link_url = link.get('url')
            if not isinstance(link_url, str) or not self._is_valid_url(link_url):
                continue

            link_number += 1

            # Proper external link detection
            is_external = self._is_external_link(link_url, base_url)

            links_rows.append({
                'page_id': page_id,
                'link_number': link_number,
                'link_url': link_url,
                'link_text': link.get('text', ''),
                'link_title': link.get('title', ''),
                'is_external': is_external,
                'scraped_at': scraped_at_utc,
            })

        # Add images table
        images_rows: List[Dict[str, Any]] = []
        image_number = 0
        for image in data.get('images', []) or []:
            if not isinstance(image, dict):
                continue

            image_src = image.get('src')
            if not isinstance(image_src, str) or not self._is_valid_url(image_src):
                continue

            image_number += 1

            # Check if image is external
            is_external_image = self._is_external_link(image_src, base_url)

            images_rows.append({
                'image_id': f"{page_id}_img_{image_number}",
                'page_id': page_id,
                'image_src': image_src,
                'image_alt': image.get('alt', ''),
                'is_external': is_external_image,
                'scraped_at': scraped_at_utc,
            })

        return {
            'pages': [page_row],
            'links': links_rows,
            'images': images_rows,
        }
    
    def _normalize_urls(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize URLs to absolute format"""
        base_url = data.get('url', '')
        if not base_url:
            return data
        
        # Normalize links
        if 'links' in data and isinstance(data['links'], list):
            normalized_links = []
            for link in data['links']:
                if not isinstance(link, dict):
                    continue

                # Get the raw href (should already be absolute from scrapers)
                raw_link_url = link.get('url') or link.get('href') or ''
                if not isinstance(raw_link_url, str):
                    raw_link_url = str(raw_link_url)

                # Skip empty, whitespace-only, or invalid URLs early
                raw_link_url = raw_link_url.strip()
                if not raw_link_url or raw_link_url.isspace():
                    continue

                # Skip common invalid patterns
                lowered = raw_link_url.lower()
                if lowered.startswith(('#', 'javascript:', 'mailto:', 'tel:', 'data:', 'ftp:', 'file:')):
                    continue

                # Ensure URL is absolute - if not, resolve with base_url
                try:
                    if not raw_link_url.startswith(('http://', 'https://')):
                        absolute_url = urljoin(base_url, raw_link_url)
                    else:
                        absolute_url = raw_link_url
                    
                    # Additional validation after URL join
                    if not self._is_valid_url(absolute_url):
                        continue
                        
                    # Ensure final URL starts with proper domain (for geeksforgeeks.org example)
                    parsed_base = urlparse(base_url)
                    parsed_absolute = urlparse(absolute_url)
                    
                    # If no netloc in absolute URL, use base domain
                    if not parsed_absolute.netloc and parsed_base.netloc:
                        absolute_url = urljoin(base_url, raw_link_url)
                        
                    # Update the link with normalized URL
                    link['url'] = absolute_url
                    normalized_links.append(link)
                    
                except Exception:
                    # Skip malformed URLs that cause exceptions
                    continue

            data['links'] = normalized_links
        
        # Normalize images
        if 'images' in data and isinstance(data['images'], list):
            normalized_images = []
            for image in data['images']:
                if not isinstance(image, dict):
                    continue
                
                raw_img_url = image.get('src') or image.get('url') or ''
                if not isinstance(raw_img_url, str):
                    raw_img_url = str(raw_img_url)
                
                raw_img_url = raw_img_url.strip()
                if not raw_img_url:
                    continue
                
                try:
                    absolute_url = urljoin(base_url, raw_img_url)
                    if absolute_url.startswith(('http://', 'https://')):
                        image['src'] = absolute_url
                        normalized_images.append(image)
                except Exception:
                    continue
                    
            data['images'] = normalized_images
        
        return data
    
    def _generate_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data statistics"""
        stats = {
            'total_fields': len(data),
            'content_length': len(data.get('content', {}).get('text', '')),
            'paragraphs_count': len(data.get('content', {}).get('paragraphs', [])),
            'headings_count': len(data.get('content', {}).get('headings', [])),
            'links_count': len(data.get('links', [])),
            'images_count': len(data.get('images', [])),
            'forms_count': len(data.get('forms', []))
        }
        
        # Add word count
        text = data.get('content', {}).get('text', '')
        stats['word_count'] = len(text.split()) if text else 0
        
        # Add character count
        stats['character_count'] = len(text)
        
        return stats
    
    def _create_error_result(self, url: str, error_message: str) -> Dict[str, Any]:
        """Create error result"""
        return {
            'url': url,
            'status': 'error',
            'error': error_message,
            'scraped_at': datetime.now(timezone.utc).isoformat(),
            'tables': {'pages': [], 'links': [], 'images': []},
            'statistics': {},
            'data': {
                'pages_scraped': 0,
                'links_found': 0,
                'images_found': 0,
                'headings_found': 0,
                'paragraphs_found': 0,
                'forms_found': 0,
                'word_count': 0,
                'character_count': 0
            },
            'validation': {'valid': False, 'errors': [error_message], 'warnings': []}
        }
