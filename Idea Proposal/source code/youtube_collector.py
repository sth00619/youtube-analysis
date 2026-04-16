from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import json
import os
from datetime import datetime

class YouTubeCollector:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.youtube = build('youtube', 'v3', developerKey=api_keys[0])
        self.quota_used = 0
        
    def rotate_api_key(self):
        """API 키 순환"""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        self.youtube = build('youtube', 'v3', 
                           developerKey=self.api_keys[self.current_key_index])
        print(f"Switched to API key {self.current_key_index + 1}")
    
    def collect_videos_batch(self, video_ids):
        """배치로 비디오 데이터 수집 (50개씩)"""
        batches = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]
        results = []
        
        for batch_num, batch in enumerate(batches, 1):
            print(f"Processing batch {batch_num}/{len(batches)}...")
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    response = self.youtube.videos().list(
                        part='statistics,contentDetails,snippet',
                        id=','.join(batch),
                        fields='items(id,statistics,contentDetails/duration,snippet(title,categoryId,publishedAt,channelId))'
                    ).execute()
                    
                    results.extend(response.get('items', []))
                    self.quota_used += 1  # 각 요청당 1 quota unit
                    break
                    
                except HttpError as e:
                    if e.resp.status == 403:  # Quota exceeded
                        print(f"Quota exceeded. Rotating API key...")
                        self.rotate_api_key()
                        retry_count += 1
                        time.sleep(60)
                    else:
                        print(f"Error: {e}")
                        retry_count += 1
                        time.sleep(30)
            
            time.sleep(1)  # Rate limiting
        
        return results
    
    def save_results(self, results, filename):
        """결과 저장"""
        output_path = f"data/raw/{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Saved {len(results)} videos to {output_path}")
        return output_path

# 실행 스크립트
if __name__ == "__main__":
    import sys
    sys.path.append('/home/alpngmu99/youtube_pipeline')
    from config.api_keys import YOUTUBE_API_KEYS
    
    # 샘플 비디오 ID 리스트 (실제로는 Kaggle 데이터에서 추출)
    sample_video_ids = ['dQw4w9WgXcQ', 'jNQXAC9IVRw']  # 테스트용
    
    collector = YouTubeCollector(YOUTUBE_API_KEYS)
    results = collector.collect_videos_batch(sample_video_ids)
    collector.save_results(results, 'youtube_api_data')