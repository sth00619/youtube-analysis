# storage/speed_storage.py
"""
Speed Layer for real-time data ingestion
Justification: Handles streaming data with low latency
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

class SpeedStorageLayer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("YouTubeSpeedLayer") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
    
    def create_streaming_table(self):
        """
        Create Delta table for streaming data
        Justification: Delta provides ACID transactions for streaming
        """
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS youtube_analytics.streaming_videos (
                video_id STRING,
                timestamp TIMESTAMP,
                views_delta BIGINT,
                likes_delta INT,
                comments_delta INT,
                processing_time TIMESTAMP
            )
            USING DELTA
            LOCATION 'gs://youtube-bigdata-storage/streaming/'
        """)
    
    def process_streaming_data(self, kafka_topic):
        """
        Process real-time updates from Kafka
        Justification: Kafka provides fault-tolerant message streaming
        """
        # Simulated streaming for demonstration
        streaming_df = self.spark \
            .readStream \
            .format("rate") \
            .option("rowsPerSecond", 10) \
            .load() \
            .withColumn("video_id", expr("uuid()")) \
            .withColumn("views_delta", expr("rand() * 1000")) \
            .withColumn("processing_time", current_timestamp())
        
        # Write to Delta table
        query = streaming_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "gs://youtube-bigdata-storage/checkpoints/") \
            .start("gs://youtube-bigdata-storage/streaming/")
        
        return query
