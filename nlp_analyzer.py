import spacy
import pandas as pd
from textstat import flesch_reading_ease, flesch_kincaid_grade
from collections import Counter
import re
import logging
from typing import Dict, List, Tuple, Optional
import numpy as np
from datetime import datetime

class NLPAnalyzer:
    def __init__(self):
        # Load spaCy model (try different models)
        self.nlp = self._load_spacy_model()
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Positive sentiment keywords
        self.positive_keywords = {
            'breakthrough', 'success', 'achievement', 'victory', 'innovation',
            'improvement', 'progress', 'growth', 'opportunity', 'positive',
            'benefit', 'advantage', 'win', 'record', 'milestone', 'launch',
            'announce', 'unveil', 'discover', 'solution', 'advance'
        }
        
        # Negative sentiment keywords
        self.negative_keywords = {
            'crisis', 'disaster', 'failure', 'crash', 'decline', 'loss',
            'problem', 'issue', 'concern', 'warning', 'threat', 'risk',
            'scandal', 'controversy', 'attack', 'conflict', 'protest',
            'investigation', 'lawsuit', 'arrest', 'death', 'injury'
        }
        
        # Trending topic categories
        self.topic_categories = {
            'technology': ['ai', 'artificial intelligence', 'tech', 'startup', 'app', 'software', 'digital'],
            'business': ['company', 'stock', 'market', 'economy', 'finance', 'investment', 'revenue'],
            'politics': ['government', 'president', 'election', 'policy', 'congress', 'senate'],
            'sports': ['team', 'player', 'game', 'championship', 'season', 'score', 'match'],
            'entertainment': ['movie', 'actor', 'music', 'celebrity', 'show', 'film', 'album'],
            'health': ['health', 'medical', 'doctor', 'hospital', 'treatment', 'vaccine', 'study']
        }
    
    def _load_spacy_model(self):
        """Load the best available spaCy model"""
        models_to_try = ['en_core_web_lg', 'en_core_web_md', 'en_core_web_sm']
        
        for model_name in models_to_try:
            try:
                nlp = spacy.load(model_name)
                print(f"Loaded spaCy model: {model_name}")
                return nlp
            except OSError:
                continue
        
        # If no model found, download and load the small model
        try:
            import subprocess
            subprocess.run(['python', '-m', 'spacy', 'download', 'en_core_web_sm'], check=True)
            nlp = spacy.load('en_core_web_sm')
            print("Downloaded and loaded en_core_web_sm")
            return nlp
        except:
            raise Exception("Could not load any spaCy model. Please install: python -m spacy download en_core_web_sm")
    
    def analyze_article(self, title: str, content: str) -> Dict:
        """Comprehensive NLP analysis of an article"""
        try:
            # Process text with spaCy
            title_doc = self.nlp(title)
            content_doc = self.nlp(content[:10000])  # Limit content length for processing
            
            analysis = {
                'sentiment': self.analyze_sentiment(title, content),
                'entities': self.extract_entities(title_doc, content_doc),
                'keywords': self.extract_keywords(content_doc),
                'readability': self.analyze_readability(content),
                'topics': self.classify_topics(title, content),
                'urgency_score': self.calculate_urgency(title, content),
                'engagement_features': self.extract_engagement_features(title, content),
                'content_quality': self.assess_content_quality(content_doc),
                'title_analysis': self.analyze_title(title_doc)
            }
            
            return analysis
        
        except Exception as e:
            self.logger.error(f"Error in NLP analysis: {str(e)}")
            return self._get_default_analysis()
    
    def analyze_sentiment(self, title: str, content: str) -> Dict:
        """Analyze sentiment using keyword-based approach and spaCy"""
        try:
            combined_text = (title + " " + content).lower()
            
            # Count positive and negative keywords
            positive_count = sum(1 for word in self.positive_keywords if word in combined_text)
            negative_count = sum(1 for word in self.negative_keywords if word in combined_text)
            
            # Calculate sentiment score
            total_sentiment_words = positive_count + negative_count
            if total_sentiment_words > 0:
                sentiment_score = (positive_count - negative_count) / total_sentiment_words
            else:
                sentiment_score = 0
            
            # Normalize to 0-1 scale
            normalized_sentiment = (sentiment_score + 1) / 2
            
            # Determine sentiment label
            if sentiment_score > 0.2:
                sentiment_label = 'positive'
            elif sentiment_score < -0.2:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
            
            return {
                'score': normalized_sentiment,
                'label': sentiment_label,
                'positive_indicators': positive_count,
                'negative_indicators': negative_count
            }
        
        except Exception as e:
            self.logger.error(f"Error in sentiment analysis: {str(e)}")
            return {'score': 0.5, 'label': 'neutral', 'positive_indicators': 0, 'negative_indicators': 0}
    
    def extract_entities(self, title_doc, content_doc) -> List[str]:
        """Extract named entities from title and content"""
        try:
            entities = []
            
            # Extract from both title and content
            for doc in [title_doc, content_doc]:
                for ent in doc.ents:
                    if ent.label_ in ['PERSON', 'ORG', 'GPE', 'EVENT', 'PRODUCT']:
                        entities.append(ent.text.strip())
            
            # Remove duplicates and filter
            unique_entities = list(set(entities))
            
            # Filter out common/generic entities
            filtered_entities = [
                ent for ent in unique_entities 
                if len(ent) > 2 and not ent.lower() in ['the', 'and', 'for', 'with', 'this', 'that']
            ]
            
            return filtered_entities[:15]  # Limit to top 15 entities
        
        except Exception as e:
            self.logger.error(f"Error extracting entities: {str(e)}")
            return []
    
    def extract_keywords(self, content_doc) -> List[str]:
        """Extract important keywords using spaCy"""
        try:
            # Get tokens that are not stop words, punctuation, or spaces
            keywords = []
            
            for token in content_doc:
                if (not token.is_stop and 
                    not token.is_punct and 
                    not token.is_space and 
                    len(token.text) > 3 and
                    token.pos_ in ['NOUN', 'ADJ', 'VERB']):
                    keywords.append(token.lemma_.lower())
            
            # Count frequency and return top keywords
            keyword_counts = Counter(keywords)
            top_keywords = [word for word, count in keyword_counts.most_common(20) if count > 1]
            
            return top_keywords
        
        except Exception as e:
            self.logger.error(f"Error extracting keywords: {str(e)}")
            return []
    
    def analyze_readability(self, content: str) -> Dict:
        """Analyze text readability"""
        try:
            # Remove extra whitespace
            clean_content = re.sub(r'\s+', ' ', content.strip())
            
            if len(clean_content) < 50:  # Too short for meaningful analysis
                return {'flesch_score': 50, 'grade_level': 8, 'readability': 'medium'}
            
            flesch_score = flesch_reading_ease(clean_content)
            grade_level = flesch_kincaid_grade(clean_content)
            
            # Determine readability level
            if flesch_score >= 70:
                readability = 'easy'
            elif flesch_score >= 50:
                readability = 'medium'
            else:
                readability = 'difficult'
            
            return {
                'flesch_score': flesch_score,
                'grade_level': grade_level,
                'readability': readability
            }
        
        except Exception as e:
            self.logger.error(f"Error in readability analysis: {str(e)}")
            return {'flesch_score': 50, 'grade_level': 8, 'readability': 'medium'}
    
    def classify_topics(self, title: str, content: str) -> List[str]:
        """Classify article into topic categories"""
        try:
            combined_text = (title + " " + content).lower()
            topics = []
            
            for category, keywords in self.topic_categories.items():
                matches = sum(1 for keyword in keywords if keyword in combined_text)
                if matches >= 2:  # Threshold for topic classification
                    topics.append(category)
            
            return topics
        
        except Exception as e:
            self.logger.error(f"Error in topic classification: {str(e)}")
            return []
    
    def calculate_urgency(self, title: str, content: str) -> float:
        """Calculate urgency score based on content indicators"""
        try:
            urgency_keywords = {
                'breaking', 'urgent', 'alert', 'immediate', 'emergency',
                'critical', 'developing', 'live', 'update', 'just in',
                'now', 'today', 'happening', 'latest'
            }
            
            combined_text = (title + " " + content).lower()
            urgency_count = sum(1 for word in urgency_keywords if word in combined_text)
            
            # Also check for time indicators
            time_patterns = ['minutes ago', 'hours ago', 'just happened', 'right now']
            time_count = sum(1 for pattern in time_patterns if pattern in combined_text)
            
            urgency_score = min((urgency_count + time_count) / 5, 1.0)
            return urgency_score
        
        except Exception as e:
            self.logger.error(f"Error calculating urgency: {str(e)}")
            return 0.0
    
    def extract_engagement_features(self, title: str, content: str) -> Dict:
        """Extract features that predict engagement"""
        try:
            features = {}
            
            # Title features
            features['title_length'] = len(title.split())
            features['has_numbers'] = bool(re.search(r'\d', title))
            features['has_question'] = '?' in title
            features['has_exclamation'] = '!' in title
            features['title_sentiment'] = self.analyze_sentiment(title, "")['score']
            
            # Content features
            features['content_length'] = len(content.split())
            features['paragraph_count'] = len(content.split('\n\n'))
            features['avg_sentence_length'] = self._calculate_avg_sentence_length(content)
            features['quotation_count'] = content.count('"')
            
            # Engagement trigger words
            trigger_words = ['you', 'your', 'how to', 'why', 'what', 'secret', 'amazing', 'incredible']
            features['trigger_word_count'] = sum(1 for word in trigger_words if word.lower() in title.lower())
            
            return features
        
        except Exception as e:
            self.logger.error(f"Error extracting engagement features: {str(e)}")
            return {}
    
    def assess_content_quality(self, content_doc) -> Dict:
        """Assess the quality of the content"""
        try:
            # Sentence structure analysis
            sentences = list(content_doc.sents)
            sentence_lengths = [len(sent.text.split()) for sent in sentences]
            
            quality_metrics = {
                'sentence_count': len(sentences),
                'avg_sentence_length': np.mean(sentence_lengths) if sentence_lengths else 0,
                'sentence_variety': np.std(sentence_lengths) if len(sentence_lengths) > 1 else 0,
                'complex_sentences': sum(1 for length in sentence_lengths if length > 20),
                'information_density': self._calculate_information_density(content_doc)
            }
            
            # Overall quality score
            quality_score = min(
                quality_metrics['information_density'] * 0.4 +
                min(quality_metrics['avg_sentence_length'] / 15, 1) * 0.3 +
                min(quality_metrics['sentence_variety'] / 10, 1) * 0.3,
                1.0
            )
            
            quality_metrics['overall_score'] = quality_score
            return quality_metrics
        
        except Exception as e:
            self.logger.error(f"Error assessing content quality: {str(e)}")
            return {'overall_score': 0.5}
    
    def analyze_title(self, title_doc) -> Dict:
        """Analyze title characteristics"""
        try:
            title_text = title_doc.text
            
            analysis = {
                'word_count': len(title_text.split()),
                'character_count': len(title_text),
                'capitalized_words': sum(1 for word in title_text.split() if word[0].isupper()),
                'has_colon': ':' in title_text,
                'power_words': self._count_power_words(title_text),
                'emotional_words': self._count_emotional_words(title_text)
            }
            
            # Title effectiveness score
            effectiveness = 0
            if 6 <= analysis['word_count'] <= 12:  # Optimal length
                effectiveness += 0.3
            if analysis['power_words'] > 0:
                effectiveness += 0.2
            if analysis['emotional_words'] > 0:
                effectiveness += 0.2
            if analysis['has_colon']:
                effectiveness += 0.1
            
            analysis['effectiveness_score'] = min(effectiveness, 1.0)
            return analysis
        
        except Exception as e:
            self.logger.error(f"Error analyzing title: {str(e)}")
            return {'effectiveness_score': 0.5}
    
    def _calculate_avg_sentence_length(self, content: str) -> float:
        """Calculate average sentence length"""
        sentences = re.split(r'[.!?]+', content)
        sentence_lengths = [len(sent.split()) for sent in sentences if sent.strip()]
        return np.mean(sentence_lengths) if sentence_lengths else 0
    
    def _calculate_information_density(self, doc) -> float:
        """Calculate information density based on entity and keyword ratio"""
        total_tokens = len([token for token in doc if not token.is_space])
        entities = len(doc.ents)
        
        if total_tokens == 0:
            return 0
        
        density = min(entities / total_tokens * 10, 1.0)  # Scale up and cap at 1.0
        return density
    
    def _count_power_words(self, text: str) -> int:
        """Count power words in text"""
        power_words = {
            'ultimate', 'essential', 'complete', 'perfect', 'amazing', 'incredible',
            'revolutionary', 'breakthrough', 'exclusive', 'proven', 'guaranteed',
            'secret', 'hidden', 'revealed', 'exposed', 'shocking'
        }
        return sum(1 for word in power_words if word in text.lower())
    
    def _count_emotional_words(self, text: str) -> int:
        """Count emotional words in text"""
        emotional_words = {
            'love', 'hate', 'fear', 'hope', 'exciting', 'thrilling', 'devastating',
            'heartbreaking', 'inspiring', 'outrageous', 'stunning', 'brilliant'
        }
        return sum(1 for word in emotional_words if word in text.lower())
    
    def _get_default_analysis(self) -> Dict:
        """Return default analysis in case of errors"""
        return {
            'sentiment': {'score': 0.5, 'label': 'neutral'},
            'entities': [],
            'keywords': [],
            'readability': {'flesch_score': 50, 'grade_level': 8, 'readability': 'medium'},
            'topics': [],
            'urgency_score': 0.0,
            'engagement_features': {},
            'content_quality': {'overall_score': 0.5},
            'title_analysis': {'effectiveness_score': 0.5}
        }
