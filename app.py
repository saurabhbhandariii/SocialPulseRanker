import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import asyncio
import threading
from news_scraper import NewsScraper
from nlp_analyzer import NLPAnalyzer
from ranking_engine import RankingEngine
from social_media_poster import SocialMediaPoster
from data_manager import DataManager
from config import Config

# Page configuration
st.set_page_config(
    page_title="NLP Social Media Content Creator",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize components
@st.cache_resource
def initialize_components():
    config = Config()
    data_manager = DataManager()
    news_scraper = NewsScraper(config)
    nlp_analyzer = NLPAnalyzer()
    ranking_engine = RankingEngine(nlp_analyzer)
    social_media_poster = SocialMediaPoster(config)
    
    return {
        'config': config,
        'data_manager': data_manager,
        'news_scraper': news_scraper,
        'nlp_analyzer': nlp_analyzer,
        'ranking_engine': ranking_engine,
        'social_media_poster': social_media_poster
    }

components = initialize_components()

# Initialize session state
if 'articles_processed_today' not in st.session_state:
    st.session_state.articles_processed_today = 0
if 'last_scrape_time' not in st.session_state:
    st.session_state.last_scrape_time = None
if 'auto_posting_enabled' not in st.session_state:
    st.session_state.auto_posting_enabled = False

def main():
    st.title("🚀 NLP Social Media Content Creator")
    st.markdown("*Automated news curation and social media posting powered by NLP*")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Control Panel")
        
        # Auto-posting toggle
        auto_posting = st.toggle(
            "Auto-posting Enabled",
            value=st.session_state.auto_posting_enabled,
            help="Automatically post top-ranked articles to social media"
        )
        st.session_state.auto_posting_enabled = auto_posting
        
        # Manual scraping button
        if st.button("🔄 Scrape News Now", use_container_width=True):
            with st.spinner("Scraping latest news..."):
                scrape_and_process_news()
        
        # Processing settings
        st.subheader("Processing Settings")
        min_score_threshold = st.slider(
            "Minimum Score for Posting",
            min_value=0.0,
            max_value=10.0,
            value=7.0,
            step=0.1
        )
        
        max_posts_per_day = st.number_input(
            "Max Posts per Day",
            min_value=1,
            max_value=20,
            value=5
        )
        
        # Data management
        st.subheader("Data Management")
        if st.button("🗑️ Clear Old Articles", use_container_width=True):
            components['data_manager'].cleanup_old_articles(days=7)
            st.success("Cleaned up articles older than 7 days")
            st.rerun()
    
    # Main dashboard
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📰 Articles", "🎯 Rankings", "📱 Social Media"])
    
    with tab1:
        show_dashboard()
    
    with tab2:
        show_articles_tab()
    
    with tab3:
        show_rankings_tab()
    
    with tab4:
        show_social_media_tab(min_score_threshold, max_posts_per_day)

def show_dashboard():
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    articles_today = components['data_manager'].get_articles_count_today()
    posted_today = components['data_manager'].get_posted_count_today()
    avg_score = components['data_manager'].get_average_score_today()
    pending_articles = components['data_manager'].get_pending_articles_count()
    
    with col1:
        st.metric("Articles Processed Today", articles_today)
    
    with col2:
        st.metric("Posts Published Today", posted_today)
    
    with col3:
        st.metric("Average Score Today", f"{avg_score:.2f}" if avg_score else "N/A")
    
    with col4:
        st.metric("Pending Articles", pending_articles)
    
    # Processing status
    st.subheader("📈 Processing Overview")
    
    # Recent activity chart
    activity_data = components['data_manager'].get_hourly_activity()
    if not activity_data.empty:
        fig = px.bar(
            activity_data,
            x='hour',
            y='count',
            title="Article Processing by Hour (Last 24h)",
            labels={'hour': 'Hour', 'count': 'Articles Processed'}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No processing activity data available yet. Start scraping to see metrics!")
    
    # Score distribution
    col1, col2 = st.columns(2)
    
    with col1:
        score_dist = components['data_manager'].get_score_distribution()
        if not score_dist.empty:
            fig = px.histogram(
                score_dist,
                x='score',
                nbins=20,
                title="Article Score Distribution",
                labels={'score': 'Engagement Score', 'count': 'Number of Articles'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No score data available yet.")
    
    with col2:
        source_data = components['data_manager'].get_source_statistics()
        if not source_data.empty:
            fig = px.pie(
                source_data,
                values='count',
                names='source',
                title="Articles by Source"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No source data available yet.")

def show_articles_tab():
    st.subheader("📰 Recent Articles")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_filter = st.date_input(
            "Filter by Date",
            value=datetime.now().date(),
            max_value=datetime.now().date()
        )
    
    with col2:
        source_options = components['data_manager'].get_available_sources()
        source_filter = st.selectbox(
            "Filter by Source",
            options=['All'] + source_options
        )
    
    with col3:
        status_filter = st.selectbox(
            "Filter by Status",
            options=['All', 'Pending', 'Posted', 'Rejected']
        )
    
    # Get filtered articles
    articles = components['data_manager'].get_articles(
        date=date_filter if date_filter else None,
        source=source_filter if source_filter != 'All' else None,
        status=status_filter if status_filter != 'All' else None
    )
    
    if not articles.empty:
        # Display articles
        for idx, article in articles.iterrows():
            with st.expander(f"📄 {article['title'][:100]}... (Score: {article['score']:.2f})"):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**Source:** {article['source']}")
                    st.write(f"**URL:** {article['url']}")
                    st.write(f"**Published:** {article['published_date']}")
                    st.write(f"**Summary:** {article['summary'][:300]}...")
                    
                    # Key features
                    if article['key_features']:
                        features = eval(article['key_features']) if isinstance(article['key_features'], str) else article['key_features']
                        st.write(f"**Key Topics:** {', '.join(features.get('entities', [])[:5])}")
                
                with col2:
                    st.metric("Engagement Score", f"{article['score']:.2f}")
                    st.write(f"**Status:** {article['status']}")
                    
                    # Action buttons
                    if article['status'] == 'pending':
                        if st.button(f"📱 Post Now", key=f"post_{article['id']}"):
                            post_article(article)
                        
                        if st.button(f"❌ Reject", key=f"reject_{article['id']}"):
                            components['data_manager'].update_article_status(article['id'], 'rejected')
                            st.success("Article rejected")
                            st.rerun()
    else:
        st.info("No articles found with the selected filters.")

def show_rankings_tab():
    st.subheader("🎯 Article Rankings & Analysis")
    
    # Top articles
    top_articles = components['data_manager'].get_top_articles(limit=10)
    
    if not top_articles.empty:
        st.write("### 🏆 Top Ranked Articles")
        
        # Create a ranking chart
        fig = px.bar(
            top_articles,
            x='score',
            y='title',
            orientation='h',
            title="Top 10 Articles by Engagement Score",
            labels={'score': 'Engagement Score', 'title': 'Article Title'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed ranking table
        st.write("### 📊 Detailed Rankings")
        display_df = top_articles[['title', 'source', 'score', 'published_date', 'status']].copy()
        display_df['title'] = display_df['title'].str[:80] + '...'
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No articles available for ranking analysis.")
    
    # Ranking factors explanation
    st.write("### 📋 Ranking Factors")
    st.markdown("""
    Our NLP-powered ranking system considers multiple factors:
    
    - **Sentiment Analysis**: Positive sentiment scores higher
    - **Entity Recognition**: Articles with named entities (people, places, organizations)
    - **Readability**: Well-structured, readable content
    - **Freshness**: Recently published articles get bonus points
    - **Content Quality**: Length, coherence, and information density
    - **Topic Relevance**: Trending topics and keywords
    """)

def show_social_media_tab(min_score_threshold, max_posts_per_day):
    st.subheader("📱 Social Media Management")
    
    # Posting queue
    st.write("### 📋 Posting Queue")
    
    queue_articles = components['data_manager'].get_articles_for_posting(
        min_score=min_score_threshold,
        limit=max_posts_per_day
    )
    
    if not queue_articles.empty:
        for idx, article in queue_articles.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"**{article['title'][:100]}...**")
                    st.write(f"Source: {article['source']} | Score: {article['score']:.2f}")
                
                with col2:
                    if st.button(f"📱 Post", key=f"queue_post_{article['id']}"):
                        post_article(article)
                
                with col3:
                    if st.button(f"⏭️ Skip", key=f"queue_skip_{article['id']}"):
                        components['data_manager'].update_article_status(article['id'], 'skipped')
                        st.rerun()
                
                st.divider()
    else:
        st.info(f"No articles in queue meeting the minimum score threshold of {min_score_threshold}")
    
    # Posted content history
    st.write("### 📚 Recent Posts")
    posted_articles = components['data_manager'].get_posted_articles(limit=5)
    
    if not posted_articles.empty:
        for idx, article in posted_articles.iterrows():
            with st.expander(f"✅ {article['title'][:80]}... (Posted: {article['posted_date']})"):
                st.write(f"**Score:** {article['score']:.2f}")
                st.write(f"**Platform:** {article.get('platform', 'Multiple')}")
                st.write(f"**Engagement:** {article.get('engagement_metrics', 'N/A')}")
    else:
        st.info("No posts published yet.")

def scrape_and_process_news():
    """Scrape news articles and process them through the NLP pipeline"""
    try:
        # Scrape articles
        articles = components['news_scraper'].scrape_all_sources()
        
        if articles:
            processed_count = 0
            for article_data in articles:
                # Analyze with NLP
                analysis = components['nlp_analyzer'].analyze_article(
                    article_data['title'],
                    article_data['content']
                )
                
                # Calculate ranking score
                score = components['ranking_engine'].calculate_score(
                    article_data['title'],
                    article_data['content'],
                    analysis
                )
                
                # Save to database
                article_data.update({
                    'score': score,
                    'key_features': analysis,
                    'status': 'pending'
                })
                
                components['data_manager'].save_article(article_data)
                processed_count += 1
            
            st.session_state.articles_processed_today += processed_count
            st.session_state.last_scrape_time = datetime.now()
            
            st.success(f"Successfully processed {processed_count} new articles!")
            
            # Auto-post if enabled
            if st.session_state.auto_posting_enabled:
                auto_post_top_articles()
        else:
            st.warning("No new articles found during scraping.")
            
    except Exception as e:
        st.error(f"Error during scraping: {str(e)}")

def post_article(article):
    """Post a single article to social media"""
    try:
        success = components['social_media_poster'].post_article(article)
        if success:
            components['data_manager'].update_article_status(article['id'], 'posted')
            st.success(f"Article posted successfully!")
        else:
            st.error("Failed to post article. Check social media configuration.")
        st.rerun()
    except Exception as e:
        st.error(f"Error posting article: {str(e)}")

def auto_post_top_articles():
    """Automatically post top-ranked articles"""
    try:
        top_articles = components['data_manager'].get_articles_for_posting(min_score=7.0, limit=3)
        posted_count = 0
        
        for idx, article in top_articles.iterrows():
            success = components['social_media_poster'].post_article(article)
            if success:
                components['data_manager'].update_article_status(article['id'], 'posted')
                posted_count += 1
        
        if posted_count > 0:
            st.success(f"Auto-posted {posted_count} articles!")
    except Exception as e:
        st.error(f"Error in auto-posting: {str(e)}")

# Auto-refresh functionality
if st.session_state.get('auto_refresh', False):
    time.sleep(60)  # Refresh every minute
    st.rerun()

if __name__ == "__main__":
    main()
