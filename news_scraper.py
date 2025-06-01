import trafilatura
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re
import time
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import feedparser

class NewsScraper:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def get_website_text_content(self, url: str) -> str:
        """
        Extract clean text content from a website using trafilatura.
        Returns the main text content that's easier to understand than raw HTML.
        """
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                text = trafilatura.extract(downloaded)
                return text if text else ""
            return ""
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {str(e)}")
            return ""
    
    def scrape_rss_feed(self, feed_url: str, source_name: str) -> List[Dict]:
        """Scrape articles from RSS feed"""
        articles = []
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:20]:  # Limit to recent 20 articles
                try:
                    # Get full content
                    content = self.get_website_text_content(entry.link)
                    
                    if content and len(content) > 100:  # Only process substantial content
                        # Parse published date
                        published_date = datetime.now()
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            published_date = datetime(*entry.published_parsed[:6])
                        
                        # Only process recent articles (last 24 hours)
                        if (datetime.now() - published_date).days <= 1:
                            article = {
                                'title': entry.title,
                                'url': entry.link,
                                'content': content,
                                'summary': entry.get('summary', content[:300]),
                                'source': source_name,
                                'published_date': published_date,
                                'scraped_date': datetime.now()
                            }
                            articles.append(article)
                
                except Exception as e:
                    self.logger.error(f"Error processing RSS entry: {str(e)}")
                    continue
                
                # Rate limiting
                time.sleep(0.5)
        
        except Exception as e:
            self.logger.error(f"Error scraping RSS feed {feed_url}: {str(e)}")
        
        return articles
    
    def scrape_news_website(self, base_url: str, source_name: str) -> List[Dict]:
        """Scrape articles from a news website by finding article links"""
        articles = []
        try:
            response = self.session.get(base_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find article links (common patterns)
            article_links = set()
            
            # Look for links with article-related keywords in href or class
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Skip non-article links
                if any(skip in href.lower() for skip in ['video', 'photo', 'gallery', 'contact', 'about', 'privacy']):
                    continue
                
                # Look for article patterns
                if any(pattern in href.lower() for pattern in ['article', 'news', 'story', '/20', 'post']):
                    full_url = urljoin(base_url, href)
                    article_links.add(full_url)
            
            # Also check for links in common article containers
            for container in soup.find_all(['article', 'div'], class_=re.compile(r'.*article.*|.*story.*|.*news.*', re.I)):
                for link in container.find_all('a', href=True):
                    full_url = urljoin(base_url, link['href'])
                    article_links.add(full_url)
            
            # Process found article links
            processed_count = 0
            for article_url in list(article_links)[:30]:  # Limit to 30 articles per source
                if processed_count >= 10:  # Further limit for demo
                    break
                
                try:
                    content = self.get_website_text_content(article_url)
                    
                    if content and len(content) > 200:
                        # Extract title from content or URL
                        title = self.extract_title_from_content(content) or self.extract_title_from_url(article_url)
                        
                        if title:
                            article = {
                                'title': title,
                                'url': article_url,
                                'content': content,
                                'summary': content[:300] + '...',
                                'source': source_name,
                                'published_date': datetime.now() - timedelta(hours=1),  # Approximate recent time
                                'scraped_date': datetime.now()
                            }
                            articles.append(article)
                            processed_count += 1
                
                except Exception as e:
                    self.logger.error(f"Error processing article {article_url}: {str(e)}")
                    continue
                
                # Rate limiting
                time.sleep(1)
        
        except Exception as e:
            self.logger.error(f"Error scraping website {base_url}: {str(e)}")
        
        return articles
    
    def extract_title_from_content(self, content: str) -> Optional[str]:
        """Extract title from article content"""
        lines = content.split('\n')
        for line in lines[:5]:  # Check first 5 lines
            line = line.strip()
            if len(line) > 20 and len(line) < 200:  # Reasonable title length
                return line
        return None
    
    def extract_title_from_url(self, url: str) -> Optional[str]:
        """Extract title from URL"""
        try:
            path = urlparse(url).path
            # Remove file extensions and split by common delimiters
            title_part = path.split('/')[-1].split('.')[0]
            title = title_part.replace('-', ' ').replace('_', ' ').title()
            return title if len(title) > 10 else None
        except:
            return None
    
    def scrape_all_sources(self) -> List[Dict]:
        """Scrape articles from all configured sources"""
        all_articles = []
        
        # RSS feeds
        rss_sources = self.config.get_rss_sources()
        for source_name, feed_url in rss_sources.items():
            self.logger.info(f"Scraping RSS feed: {source_name}")
            articles = self.scrape_rss_feed(feed_url, source_name)
            all_articles.extend(articles)
            self.logger.info(f"Found {len(articles)} articles from {source_name}")
        
        # News websites
        website_sources = self.config.get_website_sources()
        for source_name, base_url in website_sources.items():
            self.logger.info(f"Scraping website: {source_name}")
            articles = self.scrape_news_website(base_url, source_name)
            all_articles.extend(articles)
            self.logger.info(f"Found {len(articles)} articles from {source_name}")
        
        # Remove duplicates based on title similarity
        unique_articles = self.remove_duplicates(all_articles)
        
        self.logger.info(f"Total unique articles scraped: {len(unique_articles)}")
        return unique_articles
    
    def remove_duplicates(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate articles based on title similarity"""
        unique_articles = []
        seen_titles = set()
        
        for article in articles:
            # Normalize title for comparison
            normalized_title = re.sub(r'[^\w\s]', '', article['title'].lower())
            title_words = set(normalized_title.split())
            
            # Check for similarity with existing titles
            is_duplicate = False
            for seen_title in seen_titles:
                seen_words = set(seen_title.split())
                
                # Calculate Jaccard similarity
                intersection = len(title_words.intersection(seen_words))
                union = len(title_words.union(seen_words))
                similarity = intersection / union if union > 0 else 0
                
                if similarity > 0.7:  # 70% similarity threshold
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_articles.append(article)
                seen_titles.add(normalized_title)
        
        return unique_articles
    
    def get_trending_topics(self) -> List[str]:
        """Get trending topics from news sources"""
        trending = []
        try:
            # Simple implementation: extract common keywords from recent headlines
            articles = self.scrape_all_sources()
            
            # Extract keywords from titles
            all_words = []
            for article in articles:
                words = re.findall(r'\b[A-Z][a-z]+\b', article['title'])
                all_words.extend(words)
            
            # Count frequency
            from collections import Counter
            word_counts = Counter(all_words)
            trending = [word for word, count in word_counts.most_common(10) if count > 1]
        
        except Exception as e:
            self.logger.error(f"Error extracting trending topics: {str(e)}")
        
        return trending
