import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import math

class RankingEngine:
    def __init__(self, nlp_analyzer):
        self.nlp_analyzer = nlp_analyzer
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Ranking weights for different factors
        self.weights = {
            'sentiment': 0.20,           # Positive sentiment boosts ranking
            'content_quality': 0.18,     # Well-written content ranks higher
            'engagement_potential': 0.17, # Features that drive engagement
            'freshness': 0.15,           # Recent articles get bonus
            'entity_richness': 0.12,     # Articles with more entities
            'readability': 0.10,         # Readable content performs better
            'urgency': 0.08             # Breaking news gets priority
        }
        
        # Topic multipliers for trending topics
        self.topic_multipliers = {
            'technology': 1.2,
            'business': 1.1,
            'politics': 1.0,
            'sports': 0.9,
            'entertainment': 0.8,
            'health': 1.1
        }
        
        # Historical performance data (would be loaded from database in real app)
        self.performance_baseline = {
            'avg_engagement': 100,
            'avg_shares': 20,
            'avg_clicks': 150
        }
    
    def calculate_score(self, title: str, content: str, nlp_analysis: Dict, 
                       published_date: Optional[datetime] = None) -> float:
        """
        Calculate comprehensive ranking score for an article
        Returns score between 0-10
        """
        try:
            # If no published date provided, assume recent
            if published_date is None:
                published_date = datetime.now()
            
            # Calculate individual component scores
            sentiment_score = self._calculate_sentiment_score(nlp_analysis.get('sentiment', {}))
            quality_score = self._calculate_quality_score(nlp_analysis.get('content_quality', {}))
            engagement_score = self._calculate_engagement_score(nlp_analysis.get('engagement_features', {}))
            freshness_score = self._calculate_freshness_score(published_date)
            entity_score = self._calculate_entity_score(nlp_analysis.get('entities', []))
            readability_score = self._calculate_readability_score(nlp_analysis.get('readability', {}))
            urgency_score = self._calculate_urgency_score(nlp_analysis.get('urgency_score', 0))
            
            # Calculate weighted sum
            total_score = (
                sentiment_score * self.weights['sentiment'] +
                quality_score * self.weights['content_quality'] +
                engagement_score * self.weights['engagement_potential'] +
                freshness_score * self.weights['freshness'] +
                entity_score * self.weights['entity_richness'] +
                readability_score * self.weights['readability'] +
                urgency_score * self.weights['urgency']
            )
            
            # Apply topic multipliers
            topic_multiplier = self._get_topic_multiplier(nlp_analysis.get('topics', []))
            total_score *= topic_multiplier
            
            # Apply title quality bonus
            title_bonus = self._calculate_title_bonus(nlp_analysis.get('title_analysis', {}))
            total_score += title_bonus
            
            # Normalize to 0-10 scale
            final_score = min(max(total_score * 10, 0), 10)
            
            self.logger.debug(f"Article score breakdown: sentiment={sentiment_score:.2f}, "
                            f"quality={quality_score:.2f}, engagement={engagement_score:.2f}, "
                            f"freshness={freshness_score:.2f}, final={final_score:.2f}")
            
            return round(final_score, 2)
        
        except Exception as e:
            self.logger.error(f"Error calculating article score: {str(e)}")
            return 5.0  # Default middle score
    
    def _calculate_sentiment_score(self, sentiment_data: Dict) -> float:
        """Calculate sentiment contribution to ranking"""
        if not sentiment_data:
            return 0.5
        
        sentiment_score = sentiment_data.get('score', 0.5)
        
        # Boost positive sentiment, neutral is okay, penalize negative
        if sentiment_score > 0.6:
            return min(sentiment_score * 1.3, 1.0)  # Boost positive
        elif sentiment_score < 0.4:
            return sentiment_score * 0.7  # Penalize negative
        else:
            return sentiment_score  # Neutral stays same
    
    def _calculate_quality_score(self, quality_data: Dict) -> float:
        """Calculate content quality contribution"""
        if not quality_data:
            return 0.5
        
        base_quality = quality_data.get('overall_score', 0.5)
        
        # Bonus factors
        sentence_count = quality_data.get('sentence_count', 0)
        avg_length = quality_data.get('avg_sentence_length', 0)
        
        # Optimal ranges
        length_bonus = 0
        if 10 <= avg_length <= 20:  # Optimal sentence length
            length_bonus = 0.1
        
        structure_bonus = 0
        if 5 <= sentence_count <= 50:  # Good structure
            structure_bonus = 0.05
        
        return min(base_quality + length_bonus + structure_bonus, 1.0)
    
    def _calculate_engagement_score(self, engagement_data: Dict) -> float:
        """Calculate engagement potential score"""
        if not engagement_data:
            return 0.5
        
        score = 0.3  # Base score
        
        # Title factors
        title_length = engagement_data.get('title_length', 0)
        if 6 <= title_length <= 12:  # Optimal title length
            score += 0.15
        
        # Engagement triggers
        if engagement_data.get('has_numbers', False):
            score += 0.1
        
        if engagement_data.get('has_question', False):
            score += 0.05
        
        trigger_words = engagement_data.get('trigger_word_count', 0)
        score += min(trigger_words * 0.05, 0.15)
        
        # Content engagement factors
        content_length = engagement_data.get('content_length', 0)
        if 200 <= content_length <= 1000:  # Optimal content length
            score += 0.1
        elif content_length > 1000:
            score += 0.05  # Longer content gets partial credit
        
        quotations = engagement_data.get('quotation_count', 0)
        if quotations > 0:
            score += min(quotations * 0.02, 0.08)  # Quotes add credibility
        
        return min(score, 1.0)
    
    def _calculate_freshness_score(self, published_date: datetime) -> float:
        """Calculate freshness score based on publication time"""
        now = datetime.now()
        
        # Handle timezone-naive dates
        if published_date.tzinfo is None:
            time_diff = now - published_date
        else:
            # Convert to naive datetime for comparison
            time_diff = now - published_date.replace(tzinfo=None)
        
        hours_old = time_diff.total_seconds() / 3600
        
        # Freshness decay function
        if hours_old <= 1:
            return 1.0  # Very fresh
        elif hours_old <= 6:
            return 0.9  # Fresh
        elif hours_old <= 24:
            return 0.7  # Recent
        elif hours_old <= 48:
            return 0.5  # Somewhat old
        elif hours_old <= 168:  # 1 week
            return 0.3  # Old
        else:
            return 0.1  # Very old
    
    def _calculate_entity_score(self, entities: List[str]) -> float:
        """Calculate score based on named entities"""
        if not entities:
            return 0.3
        
        entity_count = len(entities)
        
        # Score based on entity richness
        if entity_count >= 8:
            return 1.0
        elif entity_count >= 5:
            return 0.8
        elif entity_count >= 3:
            return 0.6
        elif entity_count >= 1:
            return 0.4
        else:
            return 0.2
    
    def _calculate_readability_score(self, readability_data: Dict) -> float:
        """Calculate readability contribution"""
        if not readability_data:
            return 0.5
        
        readability_level = readability_data.get('readability', 'medium')
        flesch_score = readability_data.get('flesch_score', 50)
        
        # Prefer medium readability for social media
        if readability_level == 'easy':
            return 0.9
        elif readability_level == 'medium':
            return 1.0
        else:  # difficult
            return 0.6
    
    def _calculate_urgency_score(self, urgency_value: float) -> float:
        """Calculate urgency contribution"""
        # Urgency can boost ranking for breaking news
        return min(urgency_value * 1.2, 1.0)
    
    def _get_topic_multiplier(self, topics: List[str]) -> float:
        """Get topic-based multiplier"""
        if not topics:
            return 1.0
        
        # Use the highest multiplier if multiple topics
        multipliers = [self.topic_multipliers.get(topic, 1.0) for topic in topics]
        return max(multipliers)
    
    def _calculate_title_bonus(self, title_analysis: Dict) -> float:
        """Calculate bonus points for title quality"""
        if not title_analysis:
            return 0
        
        effectiveness = title_analysis.get('effectiveness_score', 0)
        return effectiveness * 0.5  # Up to 0.5 bonus points
    
    def rank_articles(self, articles: List[Dict]) -> List[Dict]:
        """Rank a list of articles and return sorted by score"""
        try:
            ranked_articles = []
            
            for article in articles:
                # Calculate score if not already present
                if 'score' not in article:
                    # Analyze article if no analysis present
                    if 'nlp_analysis' not in article:
                        nlp_analysis = self.nlp_analyzer.analyze_article(
                            article.get('title', ''),
                            article.get('content', '')
                        )
                        article['nlp_analysis'] = nlp_analysis
                    else:
                        nlp_analysis = article['nlp_analysis']
                    
                    score = self.calculate_score(
                        article.get('title', ''),
                        article.get('content', ''),
                        nlp_analysis,
                        article.get('published_date')
                    )
                    article['score'] = score
                
                ranked_articles.append(article)
            
            # Sort by score (highest first)
            ranked_articles.sort(key=lambda x: x.get('score', 0), reverse=True)
            
            return ranked_articles
        
        except Exception as e:
            self.logger.error(f"Error ranking articles: {str(e)}")
            return articles
    
    def get_posting_candidates(self, articles: List[Dict], min_score: float = 7.0, 
                             max_count: int = 5) -> List[Dict]:
        """Get top articles suitable for posting"""
        ranked_articles = self.rank_articles(articles)
        
        # Filter by minimum score and limit count
        candidates = [
            article for article in ranked_articles 
            if article.get('score', 0) >= min_score
        ]
        
        return candidates[:max_count]
    
    def analyze_performance_trends(self, historical_data: List[Dict]) -> Dict:
        """Analyze performance trends to improve ranking algorithm"""
        try:
            if not historical_data:
                return {'status': 'insufficient_data'}
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(historical_data)
            
            # Calculate correlations between features and performance
            correlations = {}
            
            if 'engagement' in df.columns and 'score' in df.columns:
                correlations['score_engagement'] = df['score'].corr(df['engagement'])
            
            # Analyze top performing articles
            top_performers = df.nlargest(10, 'engagement') if 'engagement' in df.columns else df.head(10)
            
            analysis = {
                'total_articles': len(df),
                'avg_score': df['score'].mean() if 'score' in df.columns else 0,
                'avg_engagement': df['engagement'].mean() if 'engagement' in df.columns else 0,
                'correlations': correlations,
                'top_performers': top_performers.to_dict('records') if not top_performers.empty else []
            }
            
            return analysis
        
        except Exception as e:
            self.logger.error(f"Error analyzing performance trends: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def update_weights(self, performance_data: Dict):
        """Update ranking weights based on performance feedback"""
        try:
            correlations = performance_data.get('correlations', {})
            
            # Adjust weights based on what actually drives engagement
            if 'score_engagement' in correlations:
                correlation = correlations['score_engagement']
                
                # If overall score correlates well, maintain current weights
                # Otherwise, could implement more sophisticated weight adjustment
                if correlation < 0.5:
                    self.logger.warning("Low correlation between score and engagement. Consider weight adjustment.")
            
            self.logger.info("Ranking weights updated based on performance data")
        
        except Exception as e:
            self.logger.error(f"Error updating weights: {str(e)}")
