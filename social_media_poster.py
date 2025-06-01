import os
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import hashlib
import re

class SocialMediaPoster:
    def __init__(self, config):
        self.config = config
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize API clients (placeholders for actual implementations)
        self.twitter_client = self._init_twitter_client()
        self.facebook_client = self._init_facebook_client()
        self.linkedin_client = self._init_linkedin_client()
        
        # Post formatting templates
        self.post_templates = {
            'twitter': {
                'max_length': 280,
                'hashtag_limit': 3,
                'template': "{title}\n\n{summary}\n\n{hashtags}\n\n{url}"
            },
            'facebook': {
                'max_length': 2000,
                'hashtag_limit': 5,
                'template': "{title}\n\n{summary}\n\n{hashtags}\n\nRead more: {url}"
            },
            'linkedin': {
                'max_length': 1300,
                'hashtag_limit': 4,
                'template': "{title}\n\n{summary}\n\n{hashtags}\n\nSource: {url}"
            }
        }
        
        # Rate limiting settings
        self.rate_limits = {
            'twitter': {'posts_per_hour': 5, 'posts_per_day': 50},
            'facebook': {'posts_per_hour': 10, 'posts_per_day': 100},
            'linkedin': {'posts_per_hour': 3, 'posts_per_day': 20}
        }
        
        # Track posting history for rate limiting
        self.posting_history = {platform: [] for platform in ['twitter', 'facebook', 'linkedin']}
    
    def _init_twitter_client(self):
        """Initialize Twitter API client"""
        try:
            # Get credentials from environment variables
            api_key = os.getenv('TWITTER_API_KEY', '')
            api_secret = os.getenv('TWITTER_API_SECRET', '')
            access_token = os.getenv('TWITTER_ACCESS_TOKEN', '')
            access_token_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET', '')
            
            if not all([api_key, api_secret, access_token, access_token_secret]):
                self.logger.warning("Twitter credentials not found in environment variables")
                return None
            
            # In a real implementation, you would initialize the actual Twitter client here
            # For now, return a mock client
            return MockTwitterClient(api_key, api_secret, access_token, access_token_secret)
        
        except Exception as e:
            self.logger.error(f"Error initializing Twitter client: {str(e)}")
            return None
    
    def _init_facebook_client(self):
        """Initialize Facebook API client"""
        try:
            access_token = os.getenv('FACEBOOK_ACCESS_TOKEN', '')
            page_id = os.getenv('FACEBOOK_PAGE_ID', '')
            
            if not all([access_token, page_id]):
                self.logger.warning("Facebook credentials not found in environment variables")
                return None
            
            return MockFacebookClient(access_token, page_id)
        
        except Exception as e:
            self.logger.error(f"Error initializing Facebook client: {str(e)}")
            return None
    
    def _init_linkedin_client(self):
        """Initialize LinkedIn API client"""
        try:
            access_token = os.getenv('LINKEDIN_ACCESS_TOKEN', '')
            
            if not access_token:
                self.logger.warning("LinkedIn credentials not found in environment variables")
                return None
            
            return MockLinkedInClient(access_token)
        
        except Exception as e:
            self.logger.error(f"Error initializing LinkedIn client: {str(e)}")
            return None
    
    def post_article(self, article: Dict, platforms: Optional[List[str]] = None) -> bool:
        """Post an article to specified social media platforms"""
        if platforms is None:
            platforms = ['twitter', 'facebook', 'linkedin']
        
        success_count = 0
        total_platforms = len(platforms)
        
        for platform in platforms:
            try:
                if self._can_post_to_platform(platform):
                    success = self._post_to_platform(article, platform)
                    if success:
                        success_count += 1
                        self._record_post(platform)
                        self.logger.info(f"Successfully posted to {platform}")
                    else:
                        self.logger.error(f"Failed to post to {platform}")
                else:
                    self.logger.warning(f"Rate limit exceeded for {platform}")
            
            except Exception as e:
                self.logger.error(f"Error posting to {platform}: {str(e)}")
        
        # Consider successful if posted to at least one platform
        return success_count > 0
    
    def _post_to_platform(self, article: Dict, platform: str) -> bool:
        """Post to a specific platform"""
        try:
            # Format the post content
            post_content = self._format_post_content(article, platform)
            
            if platform == 'twitter' and self.twitter_client:
                return self.twitter_client.post_tweet(post_content)
            elif platform == 'facebook' and self.facebook_client:
                return self.facebook_client.post_update(post_content)
            elif platform == 'linkedin' and self.linkedin_client:
                return self.linkedin_client.post_update(post_content)
            else:
                self.logger.warning(f"No client available for {platform}")
                return False
        
        except Exception as e:
            self.logger.error(f"Error posting to {platform}: {str(e)}")
            return False
    
    def _format_post_content(self, article: Dict, platform: str) -> str:
        """Format article content for specific platform"""
        try:
            template_config = self.post_templates[platform]
            template = template_config['template']
            max_length = template_config['max_length']
            hashtag_limit = template_config['hashtag_limit']
            
            # Extract data from article
            title = article.get('title', 'Interesting Article')
            url = article.get('url', '')
            content = article.get('content', '')
            
            # Generate summary
            summary = self._generate_summary(content, platform)
            
            # Generate hashtags
            hashtags = self._generate_hashtags(article, hashtag_limit)
            
            # Format the post
            post_content = template.format(
                title=title,
                summary=summary,
                hashtags=' '.join(hashtags),
                url=url
            )
            
            # Truncate if necessary
            if len(post_content) > max_length:
                post_content = self._truncate_post(post_content, max_length)
            
            return post_content
        
        except Exception as e:
            self.logger.error(f"Error formatting post content: {str(e)}")
            return f"Check out this interesting article: {article.get('url', '')}"
    
    def _generate_summary(self, content: str, platform: str) -> str:
        """Generate a summary appropriate for the platform"""
        if platform == 'twitter':
            max_summary_length = 100
        elif platform == 'facebook':
            max_summary_length = 200
        else:  # linkedin
            max_summary_length = 150
        
        # Simple summary generation - take first few sentences
        sentences = content.split('. ')
        summary = ''
        
        for sentence in sentences:
            if len(summary + sentence + '. ') <= max_summary_length:
                summary += sentence + '. '
            else:
                break
        
        # Fallback to truncation if no sentences fit
        if not summary.strip():
            summary = content[:max_summary_length] + '...'
        
        return summary.strip()
    
    def _generate_hashtags(self, article: Dict, limit: int) -> List[str]:
        """Generate relevant hashtags for the article"""
        hashtags = []
        
        try:
            # Extract from NLP analysis if available
            nlp_analysis = article.get('nlp_analysis', {})
            topics = nlp_analysis.get('topics', [])
            entities = nlp_analysis.get('entities', [])
            keywords = nlp_analysis.get('keywords', [])
            
            # Topic-based hashtags
            topic_hashtags = {
                'technology': '#Tech',
                'business': '#Business',
                'politics': '#Politics',
                'sports': '#Sports',
                'entertainment': '#Entertainment',
                'health': '#Health'
            }
            
            for topic in topics:
                if topic in topic_hashtags:
                    hashtags.append(topic_hashtags[topic])
            
            # Entity-based hashtags (for companies, people, etc.)
            for entity in entities[:2]:  # Limit to first 2 entities
                # Clean entity name for hashtag
                clean_entity = re.sub(r'[^\w]', '', entity)
                if len(clean_entity) > 3 and len(clean_entity) < 20:
                    hashtags.append(f'#{clean_entity}')
            
            # Keyword-based hashtags
            relevant_keywords = ['news', 'breaking', 'update', 'latest']
            for keyword in keywords:
                if keyword.lower() in relevant_keywords:
                    hashtags.append(f'#{keyword.capitalize()}')
            
            # Always include a general news hashtag
            if '#News' not in hashtags:
                hashtags.append('#News')
            
            # Remove duplicates and limit
            unique_hashtags = list(dict.fromkeys(hashtags))  # Preserve order
            return unique_hashtags[:limit]
        
        except Exception as e:
            self.logger.error(f"Error generating hashtags: {str(e)}")
            return ['#News']
    
    def _truncate_post(self, post_content: str, max_length: int) -> str:
        """Intelligently truncate post content"""
        if len(post_content) <= max_length:
            return post_content
        
        # Try to truncate at sentence boundary
        truncated = post_content[:max_length-3]  # Leave space for "..."
        
        # Find last sentence ending
        last_period = truncated.rfind('.')
        last_newline = truncated.rfind('\n')
        
        cut_point = max(last_period, last_newline)
        
        if cut_point > max_length * 0.7:  # If we can preserve at least 70% of content
            return truncated[:cut_point+1] + '...'
        else:
            return truncated + '...'
    
    def _can_post_to_platform(self, platform: str) -> bool:
        """Check if we can post to platform based on rate limits"""
        if platform not in self.rate_limits:
            return False
        
        now = datetime.now()
        platform_history = self.posting_history[platform]
        
        # Clean old entries
        self.posting_history[platform] = [
            post_time for post_time in platform_history 
            if (now - post_time).total_seconds() < 86400  # Keep last 24 hours
        ]
        
        # Check hourly limit
        hour_ago = now - timedelta(hours=1)
        posts_last_hour = sum(1 for post_time in self.posting_history[platform] if post_time > hour_ago)
        
        if posts_last_hour >= self.rate_limits[platform]['posts_per_hour']:
            return False
        
        # Check daily limit
        posts_today = len(self.posting_history[platform])
        if posts_today >= self.rate_limits[platform]['posts_per_day']:
            return False
        
        return True
    
    def _record_post(self, platform: str):
        """Record a successful post for rate limiting"""
        self.posting_history[platform].append(datetime.now())
    
    def get_posting_stats(self) -> Dict:
        """Get posting statistics"""
        stats = {}
        
        for platform in ['twitter', 'facebook', 'linkedin']:
            platform_history = self.posting_history[platform]
            now = datetime.now()
            
            # Clean old entries
            recent_posts = [
                post_time for post_time in platform_history 
                if (now - post_time).total_seconds() < 86400
            ]
            
            hour_ago = now - timedelta(hours=1)
            posts_last_hour = sum(1 for post_time in recent_posts if post_time > hour_ago)
            
            stats[platform] = {
                'posts_today': len(recent_posts),
                'posts_last_hour': posts_last_hour,
                'can_post': self._can_post_to_platform(platform),
                'daily_limit': self.rate_limits[platform]['posts_per_day'],
                'hourly_limit': self.rate_limits[platform]['posts_per_hour']
            }
        
        return stats
    
    def schedule_post(self, article: Dict, scheduled_time: datetime, platforms: List[str]) -> str:
        """Schedule a post for later (simplified implementation)"""
        # In a real implementation, this would integrate with a job scheduler
        # For now, just return a mock schedule ID
        schedule_id = hashlib.md5(f"{article['id']}{scheduled_time}".encode()).hexdigest()[:8]
        
        self.logger.info(f"Scheduled post {schedule_id} for {scheduled_time}")
        return schedule_id


# Mock client implementations
class MockTwitterClient:
    def __init__(self, api_key, api_secret, access_token, access_token_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.logger = logging.getLogger(__name__)
    
    def post_tweet(self, content: str) -> bool:
        """Mock Twitter posting"""
        try:
            # Simulate API call
            self.logger.info(f"Mock Twitter post: {content[:50]}...")
            time.sleep(0.5)  # Simulate network delay
            return True
        except Exception as e:
            self.logger.error(f"Mock Twitter error: {str(e)}")
            return False


class MockFacebookClient:
    def __init__(self, access_token, page_id):
        self.access_token = access_token
        self.page_id = page_id
        self.logger = logging.getLogger(__name__)
    
    def post_update(self, content: str) -> bool:
        """Mock Facebook posting"""
        try:
            self.logger.info(f"Mock Facebook post: {content[:50]}...")
            time.sleep(0.5)
            return True
        except Exception as e:
            self.logger.error(f"Mock Facebook error: {str(e)}")
            return False


class MockLinkedInClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.logger = logging.getLogger(__name__)
    
    def post_update(self, content: str) -> bool:
        """Mock LinkedIn posting"""
        try:
            self.logger.info(f"Mock LinkedIn post: {content[:50]}...")
            time.sleep(0.5)
            return True
        except Exception as e:
            self.logger.error(f"Mock LinkedIn error: {str(e)}")
            return False
