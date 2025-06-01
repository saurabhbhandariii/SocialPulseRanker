import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Any
import os
import hashlib

class DataManager:
    def __init__(self, db_path: str = "articles.db"):
        self.db_path = db_path
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database with required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Articles table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS articles (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        url TEXT UNIQUE NOT NULL,
                        content TEXT,
                        summary TEXT,
                        source TEXT,
                        published_date DATETIME,
                        scraped_date DATETIME,
                        score REAL,
                        status TEXT DEFAULT 'pending',
                        key_features TEXT,
                        nlp_analysis TEXT,
                        posted_date DATETIME,
                        platforms_posted TEXT,
                        engagement_metrics TEXT
                    )
                ''')
                
                # Processing stats table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS processing_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date DATE,
                        hour INTEGER,
                        articles_processed INTEGER DEFAULT 0,
                        articles_posted INTEGER DEFAULT 0,
                        avg_score REAL
                    )
                ''')
                
                # Social media posts table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS social_posts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        article_id TEXT,
                        platform TEXT,
                        post_content TEXT,
                        posted_date DATETIME,
                        engagement_count INTEGER DEFAULT 0,
                        FOREIGN KEY (article_id) REFERENCES articles (id)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_date ON articles(published_date)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_score ON articles(score)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status)')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
        
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def save_article(self, article_data: Dict) -> str:
        """Save an article to the database"""
        try:
            # Generate unique ID for the article
            article_id = self._generate_article_id(article_data)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if article already exists
                cursor.execute('SELECT id FROM articles WHERE url = ?', (article_data['url'],))
                existing = cursor.fetchone()
                
                if existing:
                    self.logger.debug(f"Article already exists: {article_data['url']}")
                    return existing[0]
                
                # Insert new article
                cursor.execute('''
                    INSERT INTO articles (
                        id, title, url, content, summary, source, published_date,
                        scraped_date, score, status, key_features, nlp_analysis
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    article_id,
                    article_data.get('title', ''),
                    article_data.get('url', ''),
                    article_data.get('content', ''),
                    article_data.get('summary', ''),
                    article_data.get('source', ''),
                    article_data.get('published_date', datetime.now()),
                    article_data.get('scraped_date', datetime.now()),
                    article_data.get('score', 0.0),
                    article_data.get('status', 'pending'),
                    json.dumps(article_data.get('key_features', {})),
                    json.dumps(article_data.get('nlp_analysis', {}))
                ))
                
                conn.commit()
                
                # Update processing stats
                self._update_processing_stats()
                
                self.logger.info(f"Saved article: {article_id}")
                return article_id
        
        except Exception as e:
            self.logger.error(f"Error saving article: {str(e)}")
            raise
    
    def get_articles(self, date: Optional[datetime] = None, source: Optional[str] = None,
                    status: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """Retrieve articles with optional filters"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = "SELECT * FROM articles WHERE 1=1"
                params = []
                
                if date:
                    query += " AND DATE(published_date) = DATE(?)"
                    params.append(date)
                
                if source:
                    query += " AND source = ?"
                    params.append(source)
                
                if status:
                    query += " AND status = ?"
                    params.append(status)
                
                query += " ORDER BY published_date DESC LIMIT ?"
                params.append(limit)
                
                df = pd.read_sql_query(query, conn, params=params)
                
                # Parse JSON fields
                if not df.empty:
                    df['key_features'] = df['key_features'].apply(
                        lambda x: json.loads(x) if x else {}
                    )
                    df['nlp_analysis'] = df['nlp_analysis'].apply(
                        lambda x: json.loads(x) if x else {}
                    )
                
                return df
        
        except Exception as e:
            self.logger.error(f"Error retrieving articles: {str(e)}")
            return pd.DataFrame()
    
    def update_article_status(self, article_id: str, status: str, 
                            posted_date: Optional[datetime] = None,
                            platforms: Optional[List[str]] = None):
        """Update article status and posting information"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                if status == 'posted' and posted_date:
                    cursor.execute('''
                        UPDATE articles 
                        SET status = ?, posted_date = ?, platforms_posted = ?
                        WHERE id = ?
                    ''', (status, posted_date, json.dumps(platforms or []), article_id))
                else:
                    cursor.execute('''
                        UPDATE articles SET status = ? WHERE id = ?
                    ''', (status, article_id))
                
                conn.commit()
                self.logger.info(f"Updated article {article_id} status to {status}")
        
        except Exception as e:
            self.logger.error(f"Error updating article status: {str(e)}")
    
    def get_articles_count_today(self) -> int:
        """Get count of articles processed today"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) FROM articles 
                    WHERE DATE(scraped_date) = DATE('now')
                ''')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting today's article count: {str(e)}")
            return 0
    
    def get_posted_count_today(self) -> int:
        """Get count of articles posted today"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) FROM articles 
                    WHERE status = 'posted' AND DATE(posted_date) = DATE('now')
                ''')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting today's posted count: {str(e)}")
            return 0
    
    def get_average_score_today(self) -> float:
        """Get average score of articles processed today"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT AVG(score) FROM articles 
                    WHERE DATE(scraped_date) = DATE('now') AND score > 0
                ''')
                result = cursor.fetchone()[0]
                return result if result else 0.0
        except Exception as e:
            self.logger.error(f"Error getting today's average score: {str(e)}")
            return 0.0
    
    def get_pending_articles_count(self) -> int:
        """Get count of pending articles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM articles WHERE status = 'pending'")
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting pending articles count: {str(e)}")
            return 0
    
    def get_hourly_activity(self) -> pd.DataFrame:
        """Get hourly activity data for the last 24 hours"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT 
                        strftime('%H', scraped_date) as hour,
                        COUNT(*) as count
                    FROM articles 
                    WHERE scraped_date >= datetime('now', '-24 hours')
                    GROUP BY strftime('%H', scraped_date)
                    ORDER BY hour
                '''
                return pd.read_sql_query(query, conn)
        except Exception as e:
            self.logger.error(f"Error getting hourly activity: {str(e)}")
            return pd.DataFrame()
    
    def get_score_distribution(self) -> pd.DataFrame:
        """Get score distribution data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT score
                    FROM articles 
                    WHERE score > 0 AND scraped_date >= datetime('now', '-7 days')
                '''
                return pd.read_sql_query(query, conn)
        except Exception as e:
            self.logger.error(f"Error getting score distribution: {str(e)}")
            return pd.DataFrame()
    
    def get_source_statistics(self) -> pd.DataFrame:
        """Get statistics by source"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT 
                        source,
                        COUNT(*) as count
                    FROM articles 
                    WHERE scraped_date >= datetime('now', '-7 days')
                    GROUP BY source
                    ORDER BY count DESC
                '''
                return pd.read_sql_query(query, conn)
        except Exception as e:
            self.logger.error(f"Error getting source statistics: {str(e)}")
            return pd.DataFrame()
    
    def get_available_sources(self) -> List[str]:
        """Get list of available sources"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT DISTINCT source FROM articles ORDER BY source')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Error getting available sources: {str(e)}")
            return []
    
    def get_top_articles(self, limit: int = 10) -> pd.DataFrame:
        """Get top-ranked articles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT id, title, source, score, published_date, status
                    FROM articles 
                    WHERE score > 0
                    ORDER BY score DESC 
                    LIMIT ?
                '''
                return pd.read_sql_query(query, conn, params=[limit])
        except Exception as e:
            self.logger.error(f"Error getting top articles: {str(e)}")
            return pd.DataFrame()
    
    def get_articles_for_posting(self, min_score: float = 7.0, limit: int = 5) -> pd.DataFrame:
        """Get articles suitable for posting"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT *
                    FROM articles 
                    WHERE status = 'pending' AND score >= ?
                    ORDER BY score DESC 
                    LIMIT ?
                '''
                df = pd.read_sql_query(query, conn, params=[min_score, limit])
                
                # Parse JSON fields
                if not df.empty:
                    df['key_features'] = df['key_features'].apply(
                        lambda x: json.loads(x) if x else {}
                    )
                
                return df
        except Exception as e:
            self.logger.error(f"Error getting articles for posting: {str(e)}")
            return pd.DataFrame()
    
    def get_posted_articles(self, limit: int = 10) -> pd.DataFrame:
        """Get recently posted articles"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT id, title, source, score, posted_date, platforms_posted
                    FROM articles 
                    WHERE status = 'posted'
                    ORDER BY posted_date DESC 
                    LIMIT ?
                '''
                return pd.read_sql_query(query, conn, params=[limit])
        except Exception as e:
            self.logger.error(f"Error getting posted articles: {str(e)}")
            return pd.DataFrame()
    
    def cleanup_old_articles(self, days: int = 30):
        """Clean up articles older than specified days"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM articles 
                    WHERE scraped_date < datetime('now', '-' || ? || ' days')
                ''', (days,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"Cleaned up {deleted_count} old articles")
                return deleted_count
        except Exception as e:
            self.logger.error(f"Error cleaning up old articles: {str(e)}")
            return 0
    
    def _generate_article_id(self, article_data: Dict) -> str:
        """Generate a unique ID for an article"""
        # Use URL and title to generate a consistent hash
        url = article_data.get('url', '')
        title = article_data.get('title', '')
        
        content_hash = hashlib.md5(f"{url}{title}".encode()).hexdigest()
        return f"art_{content_hash[:12]}"
    
    def _update_processing_stats(self):
        """Update processing statistics"""
        try:
            now = datetime.now()
            current_date = now.date()
            current_hour = now.hour
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if entry exists for current date/hour
                cursor.execute('''
                    SELECT id FROM processing_stats 
                    WHERE date = ? AND hour = ?
                ''', (current_date, current_hour))
                
                if cursor.fetchone():
                    # Update existing entry
                    cursor.execute('''
                        UPDATE processing_stats 
                        SET articles_processed = articles_processed + 1
                        WHERE date = ? AND hour = ?
                    ''', (current_date, current_hour))
                else:
                    # Create new entry
                    cursor.execute('''
                        INSERT INTO processing_stats (date, hour, articles_processed)
                        VALUES (?, ?, 1)
                    ''', (current_date, current_hour))
                
                conn.commit()
        
        except Exception as e:
            self.logger.error(f"Error updating processing stats: {str(e)}")
    
    def get_database_stats(self) -> Dict:
        """Get overall database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total articles
                cursor.execute('SELECT COUNT(*) FROM articles')
                total_articles = cursor.fetchone()[0]
                
                # Articles by status
                cursor.execute('''
                    SELECT status, COUNT(*) 
                    FROM articles 
                    GROUP BY status
                ''')
                status_counts = dict(cursor.fetchall())
                
                # Database size
                db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                
                return {
                    'total_articles': total_articles,
                    'status_breakdown': status_counts,
                    'database_size_mb': round(db_size / (1024 * 1024), 2)
                }
        
        except Exception as e:
            self.logger.error(f"Error getting database stats: {str(e)}")
            return {}
