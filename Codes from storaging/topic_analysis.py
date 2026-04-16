# analytics/topic_analysis.py
"""
Analysis for 4 Research Sub-topics
"""

class YouTubeTopicAnalyzer:
    def __init__(self, spark):
        self.spark = spark
        
    def analyze_topic1_viral_factors(self):
        """
        Topic 1: What makes videos go viral?
        Justification: Identifies key features correlating with high view counts
        """
        viral_analysis = self.spark.sql("""
            WITH viral_videos AS (
                SELECT 
                    *,
                    CASE 
                        WHEN views > PERCENTILE_APPROX(views, 0.95) OVER() THEN 'Viral'
                        WHEN views > PERCENTILE_APPROX(views, 0.75) OVER() THEN 'Popular'
                        ELSE 'Normal'
                    END as viral_category
                FROM youtube_analytics.fact_videos
            )
            SELECT 
                viral_category,
                AVG(LENGTH(title)) as avg_title_length,
                AVG(SIZE(tags)) as avg_tag_count,
                AVG(engagement_rate) as avg_engagement,
                AVG(CASE WHEN title RLIKE '[A-Z]{2,}' THEN 1 ELSE 0 END) as has_caps_ratio,
                AVG(CASE WHEN title RLIKE '[!?]' THEN 1 ELSE 0 END) as has_punctuation_ratio,
                COUNT(*) as video_count
            FROM viral_videos
            GROUP BY viral_category
        """)
        
        return viral_analysis
    
    def analyze_topic2_optimal_timing(self):
        """
        Topic 2: Optimal posting time for maximum reach
        Justification: Identifies temporal patterns for engagement optimization
        """
        timing_analysis = self.spark.sql("""
            WITH time_performance AS (
                SELECT 
                    EXTRACT(HOUR FROM publish_time) as hour,
                    EXTRACT(DOW FROM publish_time) as day_of_week,
                    country,
                    AVG(views) as avg_views,
                    AVG(engagement_rate) as avg_engagement,
                    COUNT(*) as sample_size,
                    PERCENTILE_APPROX(views, 0.75) as p75_views
                FROM youtube_analytics.fact_videos
                GROUP BY 1, 2, 3
                HAVING sample_size > 30
            )
            SELECT 
                hour,
                day_of_week,
                AVG(avg_views) as global_avg_views,
                AVG(avg_engagement) as global_avg_engagement,
                RANK() OVER (ORDER BY AVG(avg_views) DESC) as view_rank,
                RANK() OVER (ORDER BY AVG(avg_engagement) DESC) as engagement_rank
            FROM time_performance
            GROUP BY hour, day_of_week
        """)
        
        return timing_analysis
    
    def analyze_topic3_category_trends(self):
        """
        Topic 3: Category popularity evolution
        Justification: Tracks category dynamics over time
        """
        category_evolution = self.spark.sql("""
            WITH monthly_categories AS (
                SELECT 
                    DATE_TRUNC('month', trending_date) as month,
                    category_id,
                    COUNT(*) as video_count,
                    SUM(views) as total_views,
                    AVG(engagement_rate) as avg_engagement
                FROM youtube_analytics.fact_videos
                GROUP BY 1, 2
            ),
            category_growth AS (
                SELECT 
                    *,
                    LAG(total_views) OVER (PARTITION BY category_id ORDER BY month) as prev_month_views,
                    (total_views - LAG(total_views) OVER (PARTITION BY category_id ORDER BY month)) 
                        / LAG(total_views) OVER (PARTITION BY category_id ORDER BY month) * 100 as growth_rate
                FROM monthly_categories
            )
            SELECT 
                month,
                category_id,
                video_count,
                total_views,
                growth_rate,
                RANK() OVER (PARTITION BY month ORDER BY total_views DESC) as monthly_rank
            FROM category_growth
            WHERE growth_rate IS NOT NULL
        """)
        
        return category_evolution
    
    def analyze_topic4_channel_strategies(self):
        """
        Topic 4: Successful channel strategies
        Justification: Identifies patterns in high-performing channels
        """
        channel_strategies = self.spark.sql("""
            WITH channel_metrics AS (
                SELECT 
                    channel_title,
                    COUNT(DISTINCT video_id) as video_count,
                    COUNT(DISTINCT category_id) as category_diversity,
                    AVG(views) as avg_views,
                    STDDEV(views) / AVG(views) as view_consistency,
                    AVG(LENGTH(title)) as avg_title_length,
                    AVG(engagement_rate) as avg_engagement,
                    MAX(views) / AVG(views) as peak_ratio
                FROM youtube_analytics.fact_videos
                GROUP BY channel_title
                HAVING video_count >= 5
            ),
            channel_performance AS (
                SELECT 
                    *,
                    CASE 
                        WHEN avg_views > PERCENTILE_APPROX(avg_views, 0.9) OVER() THEN 'Top Performer'
                        WHEN avg_views > PERCENTILE_APPROX(avg_views, 0.5) OVER() THEN 'Average'
                        ELSE 'Below Average'
                    END as performance_tier
                FROM channel_metrics
            )
            SELECT 
                performance_tier,
                AVG(category_diversity) as avg_category_diversity,
                AVG(view_consistency) as avg_consistency,
                AVG(avg_title_length) as avg_title_length,
                AVG(peak_ratio) as avg_peak_ratio,
                COUNT(*) as channel_count
            FROM channel_performance
            GROUP BY performance_tier
        """)
        
        return channel_strategies
