-- Hive 데이터베이스 생성
CREATE DATABASE IF NOT EXISTS youtube_analytics;
USE youtube_analytics;

-- 1. 외부 테이블 생성 (Raw Data)
CREATE EXTERNAL TABLE IF NOT EXISTS youtube_trending_raw (
    video_id STRING,
    trending_date DATE,
    title STRING,
    channel_title STRING,
    category_id INT,
    publish_time TIMESTAMP,
    tags STRING,
    views BIGINT,
    likes INT,
    dislikes INT,
    comment_count INT,
    thumbnail_link STRING,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description STRING,
    country STRING
)
STORED AS TEXTFILE
LOCATION 'gs://youtube-bigdata-storage/raw/kaggle/';

-- 2. ORC 포맷 파티션 테이블 생성
CREATE TABLE IF NOT EXISTS youtube_trending_orc (
    video_id STRING,
    title STRING,
    channel_title STRING,
    category_id INT,
    publish_time TIMESTAMP,
    tags STRING,
    views BIGINT,
    likes INT,
    dislikes INT,
    comment_count INT,
    engagement_rate DOUBLE,
    like_ratio DOUBLE,
    description STRING
)
PARTITIONED BY (
    trending_date DATE,
    country STRING
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='67108864',
    'orc.row.index.stride'='10000'
);

-- 3. 채널 통계 테이블
CREATE TABLE IF NOT EXISTS channel_statistics (
    channel_title STRING,
    total_videos INT,
    total_views BIGINT,
    avg_views DOUBLE,
    total_likes BIGINT,
    avg_engagement_rate DOUBLE,
    top_category INT
)
PARTITIONED BY (country STRING)
STORED AS ORC;

-- 4. 카테고리 트렌드 테이블
CREATE TABLE IF NOT EXISTS category_trends (
    category_id INT,
    category_name STRING,
    video_count INT,
    total_views BIGINT,
    avg_views DOUBLE,
    avg_likes INT,
    avg_engagement DOUBLE,
    trending_score DOUBLE
)
PARTITIONED BY (
    year INT,
    month INT,
    country STRING
)
STORED AS ORC;

-- 5. 시계열 분석 테이블
CREATE TABLE IF NOT EXISTS time_series_metrics (
    date_key DATE,
    hour_of_day INT,
    day_of_week INT,
    video_count INT,
    total_views BIGINT,
    avg_views DOUBLE,
    peak_video_id STRING,
    peak_video_views BIGINT
)
PARTITIONED BY (country STRING)
CLUSTERED BY (date_key) INTO 10 BUCKETS
STORED AS ORC;

