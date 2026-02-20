"""
Configuration management for the web scraper
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any

class Config:
    """Configuration class for the web scraper"""
    
    def __init__(self):
        self.config_dir = Path('config')
        self.config_dir.mkdir(exist_ok=True)
        self.config_file = self.config_dir / 'scraping_config.yaml'
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or create default"""
        default_config = {
            'scraping': {
                'default_delay': 1.0,
                'timeout': 30,
                'max_retries': 3,
                'user_agent': 'EthicalWebScraper/1.0 (Educational Purpose)',
                'respect_robots': True,
                'rate_limit': {
                    'requests_per_second': 1,
                    'burst_size': 5,
                    'delay_after_burst': 5.0
                }
            },
            'dynamic_scraping': {
                'headless': True,
                'wait_strategy': 'networkidle',
                'max_scroll_attempts': 5,
                'scroll_delay': 1000
            },
            'selectors': {
                'title': ['title', 'h1', '[property="og:title"]', '.title'],
                'content': ['main', 'article', '.content', '#content'],
                'links': ['a[href]'],
                'images': ['img[src]'],
                'headings': ['h1', 'h2', 'h3', 'h4', 'h5', 'h6'],
                'paragraphs': ['p'],
                'lists': ['ul', 'ol']
            },
            'data_processing': {
                'clean_text': True,
                'remove_duplicates': True,
                'validate_fields': True,
                'normalize_urls': True
            },
            'captcha_detection': {
                'enabled': True,
                'stop_on_captcha': True,
                'indicators': [
                    'captcha', 'recaptcha', 'hcaptcha',
                    'verify', 'prove', 'robot', 'human'
                ]
            }
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_config = yaml.safe_load(f)
                    # Merge with defaults
                    return self._merge_configs(default_config, loaded_config)
            except Exception as e:
                print(f"Error loading config: {e}")
                return default_config
        else:
            # Create default config file
            self.save_config(default_config)
            return default_config
    
    def _merge_configs(self, default: Dict[str, Any], loaded: Dict[str, Any]) -> Dict[str, Any]:
        """Merge loaded config with defaults"""
        result = default.copy()
        for key, value in loaded.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        return result
    
    def save_config(self, config: Dict[str, Any]):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def get(self, key: str, default=None):
        """Get configuration value by key (supports dot notation)"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value by key (supports dot notation)"""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
        self.save_config(self._config)
