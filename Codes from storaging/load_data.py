#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Spark 세션 생성 (최적화 설정 포함)"""
    return SparkSession.builder \
        .appName("YouTubeDataStorage") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.orc.splits.include.file.footer", "true") \
        .config("spark.sql.orc.cache.stripe.details.size", "10000") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "10") \
        .enableHiveSupport() \
        .getOrCreate()

def load_and_transform_data(spark):
    """데이터 로드 및 변환"""
    
    # 1. CSV 데이터 로드
    print("Loading CSV data from GCS...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("gs://youtube-bigdata-storage/processed/kaggle_youtube_cleaned.csv")
    
    # 2. 데이터 타입 최적화
    df = df.withColumn("views", col("views").cast(LongType())) \
           .withColumn("likes", col("likes").cast(IntegerType())) \
           .withColumn("comment_count", col("comment_count").cast(IntegerType())) \
           .withColumn("trending_date", to_date(col("trending_date"))) \
           .withColumn("publish_time", to_timestamp(col("publish_time")))
    
    # 3. 파생 메트릭 계산
    df = df.withColumn("engagement_rate", 
                      (col("likes") + col("comment_count")) / col("views")) \
           .withColumn("like_ratio", 
                      col("likes") / (col("likes") + col("dislikes")))
    
    # 4. 국가 코드 추출 (source_file에서)
    df = df.withColumn("country", 
                      regexp_extract(col("source_file"), r"([A-Z]{2})videos\.csv", 1))
    
    return df

def create_partitioned_table(spark, df):
    """파티션된 ORC 테이블 생성"""
    
    print("Creating partitioned ORC table...")
    
    # 동적 파티션 활성화
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    # 파티션별로 데이터 쓰기
    df.write \
        .mode("overwrite") \
        .partitionBy("trending_date", "country") \
        .format("orc") \
        .option("compression", "snappy") \
        .saveAsTable("youtube_analytics.youtube_trending_orc")
    
    print("✅ Partitioned table created")

def create_channel_statistics(spark):
    """채널별 통계 테이블 생성"""
    
    print("Creating channel statistics...")
    
    channel_stats = spark.sql("""
        SELECT 
            channel_title,
            country,
            COUNT(DISTINCT video_id) as total_videos,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            SUM(likes) as total_likes,
            AVG(engagement_rate) as avg_engagement_rate,
            MODE(category_id) as top_category
        FROM youtube_analytics.youtube_trending_orc
        GROUP BY channel_title, country
    """)
    
    channel_stats.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .format("orc") \
        .saveAsTable("youtube_analytics.channel_statistics")
    
    print("✅ Channel statistics created")

def create_category_trends(spark):
    """카테고리 트렌드 테이블 생성"""
    
    print("Creating category trends...")
    
    category_trends = spark.sql("""
        WITH category_metrics AS (
            SELECT 
                category_id,
                category_name,
                YEAR(trending_date) as year,
                MONTH(trending_date) as month,
                country,
                COUNT(*) as video_count,
                SUM(views) as total_views,
                AVG(views) as avg_views,
                AVG(likes) as avg_likes,
                AVG(engagement_rate) as avg_engagement,
                -- 트렌딩 스코어 계산
                (SUM(views) / COUNT(*)) * AVG(engagement_rate) * 1000 as trending_score
            FROM youtube_analytics.youtube_trending_orc
            GROUP BY category_id, category_name, 
                     YEAR(trending_date), MONTH(trending_date), country
        )
        SELECT * FROM category_metrics
    """)
    
    category_trends.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "country") \
        .format("orc") \
        .saveAsTable("youtube_analytics.category_trends")
    
    print("✅ Category trends created")

def create_time_series_metrics(spark):
    """시계열 메트릭 테이블 생성"""
    
    print("Creating time series metrics...")
    
    # Window 함수를 사용한 고급 분석
    time_series = spark.sql("""
        WITH daily_metrics AS (
            SELECT 
                trending_date as date_key,
                HOUR(publish_time) as hour_of_day,
                DAYOFWEEK(trending_date) as day_of_week,
                country,
                COUNT(*) as video_count,
                SUM(views) as total_views,
                AVG(views) as avg_views,
                FIRST_VALUE(video_id) OVER (
                    PARTITION BY trending_date, country 
                    ORDER BY views DESC
                ) as peak_video_id,
                MAX(views) OVER (
                    PARTITION BY trending_date, country
                ) as peak_video_views
            FROM youtube_analytics.youtube_trending_orc
            GROUP BY trending_date, HOUR(publish_time), 
                     DAYOFWEEK(trending_date), country, video_id, views
        )
        SELECT 
            date_key,
            hour_of_day,
            day_of_week,
            country,
            SUM(video_count) as video_count,
            SUM(total_views) as total_views,
            AVG(avg_views) as avg_views,
            FIRST(peak_video_id) as peak_video_id,
            MAX(peak_video_views) as peak_video_views
        FROM daily_metrics
        GROUP BY date_key, hour_of_day, day_of_week, country
    """)
    
    # Bucketing으로 조인 최적화
    time_series.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .bucketBy(10, "date_key") \
        .format("orc") \
        .saveAsTable("youtube_analytics.time_series_metrics")
    
    print("✅ Time series metrics created")

def optimize_tables(spark):
    """테이블 최적화"""
    
    print("Optimizing tables...")
    
    # 통계 수집
    spark.sql("ANALYZE TABLE youtube_analytics.youtube_trending_orc COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE youtube_analytics.channel_statistics COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE youtube_analytics.category_trends COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE youtube_analytics.time_series_metrics COMPUTE STATISTICS")
    
    # 캐싱
    spark.sql("CACHE TABLE youtube_analytics.youtube_trending_orc")
    
    print("✅ Table optimization complete")

def main():
    """메인 실행 함수"""
    
    print("="*60)
    print("YouTube Data Storage & Processing Pipeline")
    print("="*60)
    
    # Spark 세션 생성
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 1. 데이터 로드 및 변환
        df = load_and_transform_data(spark)
        
        # 2. 파티션 테이블 생성
        create_partitioned_table(spark, df)
        
        # 3. 집계 테이블 생성
        create_channel_statistics(spark)
        create_category_trends(spark)
        create_time_series_metrics(spark)
        
        # 4. 최적화
        optimize_tables(spark)
        
        # 5. 결과 확인
        print("\n" + "="*60)
        print("Storage Summary")
        print("="*60)
        
        spark.sql("""
            SELECT 
                COUNT(DISTINCT video_id) as unique_videos,
                COUNT(*) as total_records,
                COUNT(DISTINCT country) as countries,
                MIN(trending_date) as earliest_date,
                MAX(trending_date) as latest_date
            FROM youtube_analytics.youtube_trending_orc
        """).show()
        
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
