import os
import json
from typing import Dict, List
import logging

class Config:
    def __init__(self):
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Default news sources
        self.default_rss_sources = {
            'Reuters': 'https://feeds.reuters.com/reuters/topNews',
            'BBC News': 'https://feeds.bbci.co.uk/news/rss.xml',
            'CNN': 'http://rss.cnn.com/rss/edition.rss',
            'AP News': 'https://apnews.com/index.rss',
            'NPR': 'https://feeds.npr.org/1001/rss.xml',
            'TechCrunch': 'https://techcrunch.com/feed/',
            'Hacker News': 'https://hnrss.org/frontpage',
            'The Verge': 'https://www.theverge.com/rss/index.xml'
        }
        
        self.default_website_sources = {
            'Google News': 'https://news.google.com/topstories?hl=en-US&gl=US&ceid=US:en',
            'Reddit World News': 'https://www.reddit.com/r/worldnews/',
            'AllSides': 'https://www.allsides.com/news',
            'Associated Press': 'https://apnews.com/',
            'Reuters': 'https://www.reuters.com/'
        }
        
        # Social media configuration
        self.social_media_config = {
            'platforms': ['twitter', 'facebook', 'linkedin'],
            'posting_schedule': {
                'frequency': 'daily',
                'max_posts_per_day': 5,
                'preferred_times': ['09:00', '12:00', '15:00', '18:00']
            },
            'content_preferences': {
                'min_score_threshold': 7.0,
                'preferred_topics': ['technology', 'business', 'health'],
                'avoid_topics': [],
                'max_content_age_hours': 24
            }
        }
        
        # NLP processing configuration
        self.nlp_config = {
            'spacy_model': 'en_core_web_sm',
            'sentiment_threshold': 0.3,
            'entity_types': ['PERSON', 'ORG', 'GPE', 'EVENT', 'PRODUCT'],
            'max_content_length': 10000,
            'min_content_length': 100
        }
        
        # Scraping configuration
        self.scraping_config = {
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'request_timeout': 10,
            'retry_attempts': 3,
            'delay_between_requests': 1,
            'max_articles_per_source': 20
        }
        
        # Ranking weights configuration
        self.ranking_weights = {
            'sentiment': 0.20,
            'content_quality': 0.18,
            'engagement_potential': 0.17,
            'freshness': 0.15,
            'entity_richness': 0.12,
            'readability': 0.10,
            'urgency': 0.08
        }
        
        # Load custom configuration if available
        self._load_custom_config()
    
    def _load_custom_config(self):
        """Load custom configuration from file or environment variables"""
        try:
            # Try to load from config file
            config_file = os.getenv('CONFIG_FILE', 'config.json')
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    custom_config = json.load(f)
                    self._merge_config(custom_config)
                    self.logger.info(f"Loaded custom configuration from {config_file}")
            
            # Override with environment variables
            self._load_env_config()
            
        except Exception as e:
            self.logger.warning(f"Could not load custom configuration: {str(e)}")
    
    def _merge_config(self, custom_config: Dict):
        """Merge custom configuration with defaults"""
        if 'rss_sources' in custom_config:
            self.default_rss_sources.update(custom_config['rss_sources'])
        
        if 'website_sources' in custom_config:
            self.default_website_sources.update(custom_config['website_sources'])
        
        if 'social_media_config' in custom_config:
            self.social_media_config.update(custom_config['social_media_config'])
        
        if 'ranking_weights' in custom_config:
            self.ranking_weights.update(custom_config['ranking_weights'])
    
    def _load_env_config(self):
        """Load configuration from environment variables"""
        # Social media API keys
        self.twitter_api_key = os.getenv('TWITTER_API_KEY', '')
        self.twitter_api_secret = os.getenv('TWITTER_API_SECRET', '')
        self.twitter_access_token = os.getenv('TWITTER_ACCESS_TOKEN', '')
        self.twitter_access_token_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET', '')
        
        self.facebook_access_token = os.getenv('FACEBOOK_ACCESS_TOKEN', '')
        self.facebook_page_id = os.getenv('FACEBOOK_PAGE_ID', '')
        
        self.linkedin_access_token = os.getenv('LINKEDIN_ACCESS_TOKEN', '')
        
        # Database configuration
        self.database_path = os.getenv('DATABASE_PATH', 'articles.db')
        
        # Scraping configuration overrides
        max_articles = os.getenv('MAX_ARTICLES_PER_SOURCE')
        if max_articles:
            try:
                self.scraping_config['max_articles_per_source'] = int(max_articles)
            except ValueError:
                pass
        
        # Ranking threshold override
        min_score = os.getenv('MIN_POSTING_SCORE')
        if min_score:
            try:
                self.social_media_config['content_preferences']['min_score_threshold'] = float(min_score)
            except ValueError:
                pass
    
    def get_rss_sources(self) -> Dict[str, str]:
        """Get RSS feed sources"""
        return self.default_rss_sources.copy()
    
    def get_website_sources(self) -> Dict[str, str]:
        """Get website sources for scraping"""
        return self.default_website_sources.copy()
    
    def get_social_media_config(self) -> Dict:
        """Get social media configuration"""
        return self.social_media_config.copy()
    
    def get_nlp_config(self) -> Dict:
        """Get NLP processing configuration"""
        return self.nlp_config.copy()
    
    def get_scraping_config(self) -> Dict:
        """Get web scraping configuration"""
        return self.scraping_config.copy()
    
    def get_ranking_weights(self) -> Dict:
        """Get ranking algorithm weights"""
        return self.ranking_weights.copy()
    
    def add_rss_source(self, name: str, url: str):
        """Add a new RSS source"""
        self.default_rss_sources[name] = url
        self.logger.info(f"Added RSS source: {name}")
    
    def remove_rss_source(self, name: str):
        """Remove an RSS source"""
        if name in self.default_rss_sources:
            del self.default_rss_sources[name]
            self.logger.info(f"Removed RSS source: {name}")
    
    def add_website_source(self, name: str, url: str):
        """Add a new website source"""
        self.default_website_sources[name] = url
        self.logger.info(f"Added website source: {name}")
    
    def remove_website_source(self, name: str):
        """Remove a website source"""
        if name in self.default_website_sources:
            del self.default_website_sources[name]
            self.logger.info(f"Removed website source: {name}")
    
    def update_ranking_weight(self, factor: str, weight: float):
        """Update ranking weight for a specific factor"""
        if factor in self.ranking_weights and 0 <= weight <= 1:
            self.ranking_weights[factor] = weight
            self.logger.info(f"Updated ranking weight for {factor}: {weight}")
        else:
            self.logger.warning(f"Invalid ranking factor or weight: {factor}, {weight}")
    
    def set_posting_threshold(self, threshold: float):
        """Set minimum score threshold for posting"""
        if 0 <= threshold <= 10:
            self.social_media_config['content_preferences']['min_score_threshold'] = threshold
            self.logger.info(f"Updated posting threshold: {threshold}")
        else:
            self.logger.warning(f"Invalid posting threshold: {threshold}")
    
    def get_api_credentials(self, platform: str) -> Dict:
        """Get API credentials for a social media platform"""
        credentials = {}
        
        if platform == 'twitter':
            credentials = {
                'api_key': self.twitter_api_key,
                'api_secret': self.twitter_api_secret,
                'access_token': self.twitter_access_token,
                'access_token_secret': self.twitter_access_token_secret
            }
        elif platform == 'facebook':
            credentials = {
                'access_token': self.facebook_access_token,
                'page_id': self.facebook_page_id
            }
        elif platform == 'linkedin':
            credentials = {
                'access_token': self.linkedin_access_token
            }
        
        return credentials
    
    def validate_configuration(self) -> Dict:
        """Validate the current configuration"""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check RSS sources
        if not self.default_rss_sources:
            validation_results['warnings'].append("No RSS sources configured")
        
        # Check social media credentials
        twitter_creds = self.get_api_credentials('twitter')
        if not all(twitter_creds.values()):
            validation_results['warnings'].append("Twitter credentials incomplete")
        
        facebook_creds = self.get_api_credentials('facebook')
        if not all(facebook_creds.values()):
            validation_results['warnings'].append("Facebook credentials incomplete")
        
        linkedin_creds = self.get_api_credentials('linkedin')
        if not linkedin_creds['access_token']:
            validation_results['warnings'].append("LinkedIn credentials incomplete")
        
        # Check ranking weights sum
        total_weight = sum(self.ranking_weights.values())
        if abs(total_weight - 1.0) > 0.01:
            validation_results['warnings'].append(f"Ranking weights sum to {total_weight:.2f}, should be 1.0")
        
        # Check database path
        if not os.path.dirname(self.database_path):
            try:
                # Try to create a test file to check write permissions
                test_file = self.database_path + '.test'
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
            except Exception:
                validation_results['errors'].append("Cannot write to database path")
                validation_results['valid'] = False
        
        return validation_results
    
    def save_config(self, filepath: str = 'config.json'):
        """Save current configuration to file"""
        try:
            config_data = {
                'rss_sources': self.default_rss_sources,
                'website_sources': self.default_website_sources,
                'social_media_config': self.social_media_config,
                'nlp_config': self.nlp_config,
                'scraping_config': self.scraping_config,
                'ranking_weights': self.ranking_weights
            }
            
            with open(filepath, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            self.logger.info(f"Configuration saved to {filepath}")
            return True
        
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
            return False
    
    def get_trending_keywords(self) -> List[str]:
        """Get trending keywords for content filtering"""
        # This could be expanded to fetch from external APIs
        return [
            'breaking', 'update', 'latest', 'developing', 'exclusive',
            'innovation', 'technology', 'AI', 'startup', 'investment',
            'market', 'economy', 'politics', 'election', 'policy'
        ]
    
    def get_content_filters(self) -> Dict:
        """Get content filtering rules"""
        return {
            'min_word_count': 50,
            'max_word_count': 5000,
            'exclude_patterns': [
                r'(?i)advertisement',
                r'(?i)sponsored',
                r'(?i)click here',
                r'(?i)buy now'
            ],
            'required_elements': ['title', 'content'],
            'language': 'en'
        }
