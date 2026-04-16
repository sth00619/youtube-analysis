import pandas as pd
import json
import os

def analyze_all_data():
    print("="*50)
    print("Final Data Analysis")
    print("="*50)
    
    # 1. Kaggle 데이터 분석
    df = pd.read_csv('data/processed/kaggle_youtube_cleaned.csv')
    
    print("\n📊 Kaggle YouTube Trending Analysis")
    print(f"Total videos: {len(df):,}")
    print(f"Unique videos: {df['video_id'].nunique():,}")
    print(f"Average views: {df['views'].mean():,.0f}")
    print(f"Top category: {df['category_name'].mode()[0]}")
    
    # 2. 국가별 통계
    print("\n🌍 Country Statistics:")
    country_stats = df.groupby('source_file').agg({
        'views': 'mean',
        'engagement_rate': 'mean',
        'video_id': 'count'
    }).round(2)
    print(country_stats.sort_values('views', ascending=False).head())
    
    # 3. 시간대별 트렌드
    df['trending_date'] = pd.to_datetime(df['trending_date'])
    monthly_trend = df.groupby(df['trending_date'].dt.to_period('M'))['views'].mean()
    
    print("\n📈 Trend Analysis Complete!")
    
    # 결과 저장
    summary = {
        'total_videos': len(df),
        'unique_videos': df['video_id'].nunique(),
        'countries': df['source_file'].nunique(),
        'avg_views': float(df['views'].mean()),
        'avg_engagement': float(df['engagement_rate'].mean())
    }
    
    with open('data/processed/analysis_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n✅ Analysis saved to data/processed/analysis_summary.json")

if __name__ == "__main__":
    analyze_all_data()
