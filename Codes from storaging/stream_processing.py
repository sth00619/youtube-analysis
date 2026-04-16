# processing/stream_processing.py
"""
Real-time Stream Processing
Justification: Processes updates within seconds for real-time dashboards
"""

class StreamProcessor:
    def __init__(self, spark):
        self.spark = spark
        
    def process_realtime_trends(self):
        """
        Real-time trend detection using structured streaming
        """
        # Read from streaming source
        stream_df = self.spark \
            .readStream \
            .format("delta") \
            .load("gs://youtube-bigdata-storage/streaming/")
        
        # Real-time aggregation with watermarks
        realtime_trends = stream_df \
            .withWatermark("processing_time", "10 minutes") \
            .groupBy(
                window("processing_time", "5 minutes", "1 minute"),
                "video_id"
            ) \
            .agg(
                sum("views_delta").alias("views_5min"),
                avg("likes_delta").alias("avg_likes_5min"),
                count("*").alias("update_count")
            )
        
        # Output to memory for dashboard
        query = realtime_trends.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("realtime_trends") \
            .start()
        
        return query
