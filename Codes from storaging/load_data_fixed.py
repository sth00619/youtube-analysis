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
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "4") \
        .enableHiveSupport() \
        .getOrCreate()

def load_and_transform_data(spark):
    """데이터 로드 및 변환"""
    
    print("Loading CSV data from GCS...")
    
    # 여러 소스에서 데이터 로드 시도
    try:
        # 압축된 파일 시도
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("gs://youtube-bigdata-storage/processed/kaggle_youtube_cleaned.csv.gz")
    except:
        try:
            # 일반 CSV 시도
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("gs://youtube-bigdata-storage/processed/*.csv")
        except:
            # 백업 위치에서 시도
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("gs://youtube-bigdata-storage/*.csv")
    
    print(f"Loaded {df.count()} rows")
    
    # 데이터 타입 최적화
    df = df.withColumn("views", col("views").cast(LongType())) \
           .withColumn("likes", col("likes").cast(IntegerType())) \
           .withColumn("dislikes", when(col("dislikes").isNotNull(), col("dislikes").cast(IntegerType())).otherwise(0)) \
           .withColumn("comment_count", col("comment_count").cast(IntegerType())) \
           .withColumn("trending_date", to_date(col("trending_date"))) \
           .withColumn("publish_time", to_timestamp(col("publish_time")))
    
    # 파생 메트릭 계산 (안전하게)
    df = df.withColumn("engagement_rate", 
                      when(col("views") > 0, 
                           (coalesce(col("likes"), lit(0)) + coalesce(col("comment_count"), lit(0))) / col("views")
                      ).otherwise(0)) \
           .withColumn("like_ratio", 
                      when((col("likes") + col("dislikes")) > 0,
                           col("likes") / (col("likes") + col("dislikes"))
                      ).otherwise(0))
    
    # 국가 코드 추출
    df = df.withColumn("country", 
                      when(col("source_file").isNotNull(),
                           regexp_extract(col("source_file"), r"([A-Z]{2})videos\.csv", 1)
                      ).otherwise("XX"))
    
    # category_name이 없으면 추가
    if "category_name" not in df.columns:
        category_mapping = {
            1: 'Film & Animation', 2: 'Autos & Vehicles',
            10: 'Music', 15: 'Pets & Animals',
            17: 'Sports', 19: 'Travel & Events',
            20: 'Gaming', 22: 'People & Blogs',
            23: 'Comedy', 24: 'Entertainment',
            25: 'News & Politics', 26: 'Howto & Style',
            27: 'Education', 28: 'Science & Technology'
        }
        
        mapping_expr = create_map([lit(x) for x in chain(*category_mapping.items())])
        df = df.withColumn("category_name", mapping_expr[col("category_id")])
    
    return df

def create_partitioned_table(spark, df):
    """파티션된 ORC 테이블 생성"""
    
    print("Creating partitioned ORC table...")
    
    # 데이터베이스 생성
    spark.sql("CREATE DATABASE IF NOT EXISTS youtube_analytics")
    spark.sql("USE youtube_analytics")
    
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
    """채널별 통계 테이블 생성 (MODE 함수 대체)"""
    
    print("Creating channel statistics...")
    
    # MODE 대신 가장 빈번한 카테고리를 찾는 다른 방법 사용
    channel_stats = spark.sql("""
        WITH channel_category AS (
            SELECT 
                channel_title,
                country,
                category_id,
                COUNT(*) as category_count,
                ROW_NUMBER() OVER (PARTITION BY channel_title, country 
                                  ORDER BY COUNT(*) DESC) as rn
            FROM youtube_analytics.youtube_trending_orc
            GROUP BY channel_title, country, category_id
        ),
        channel_metrics AS (
            SELECT 
                channel_title,
                country,
                COUNT(DISTINCT video_id) as total_videos,
                SUM(views) as total_views,
                AVG(views) as avg_views,
                SUM(likes) as total_likes,
                AVG(engagement_rate) as avg_engagement_rate
            FROM youtube_analytics.youtube_trending_orc
            GROUP BY channel_title, country
        )
        SELECT 
            cm.*,
            cc.category_id as top_category
        FROM channel_metrics cm
        LEFT JOIN channel_category cc
            ON cm.channel_title = cc.channel_title 
            AND cm.country = cc.country
            AND cc.rn = 1
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
        SELECT 
            category_id,
            FIRST(category_name) as category_name,
            YEAR(trending_date) as year,
            MONTH(trending_date) as month,
            country,
            COUNT(*) as video_count,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            AVG(likes) as avg_likes,
            AVG(engagement_rate) as avg_engagement,
            (SUM(views) / COUNT(*)) * AVG(engagement_rate) * 1000 as trending_score
        FROM youtube_analytics.youtube_trending_orc
        WHERE category_id IS NOT NULL
        GROUP BY category_id, YEAR(trending_date), MONTH(trending_date), country
    """)
    
    category_trends.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "country") \
        .format("orc") \
        .saveAsTable("youtube_analytics.category_trends")
    
    print("✅ Category trends created")

def create_time_series_metrics(spark):
    """시계열 메트릭 테이블 생성 (단순화)"""
    
    print("Creating time series metrics...")
    
    time_series = spark.sql("""
        SELECT 
            trending_date as date_key,
            EXTRACT(HOUR FROM publish_time) as hour_of_day,
            DAYOFWEEK(trending_date) as day_of_week,
            country,
            COUNT(*) as video_count,
            SUM(views) as total_views,
            AVG(views) as avg_views,
            MAX(views) as max_views
        FROM youtube_analytics.youtube_trending_orc
        WHERE trending_date IS NOT NULL
        GROUP BY trending_date, EXTRACT(HOUR FROM publish_time), 
                 DAYOFWEEK(trending_date), country
    """)
    
    time_series.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .format("orc") \
        .saveAsTable("youtube_analytics.time_series_metrics")
    
    print("✅ Time series metrics created")

def main():
    """메인 실행 함수"""
    
    print("="*60)
    print("YouTube Data Storage & Processing Pipeline")
    print("="*60)
    
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
        
        # 4. 결과 확인
        print("\n" + "="*60)
        print("Storage Summary")
        print("="*60)
        
        result = spark.sql("""
            SELECT 
                COUNT(DISTINCT video_id) as unique_videos,
                COUNT(*) as total_records,
                COUNT(DISTINCT country) as countries,
                MIN(trending_date) as earliest_date,
                MAX(trending_date) as latest_date
            FROM youtube_analytics.youtube_trending_orc
        """)
        
        result.show()
        
        print("✅ Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
