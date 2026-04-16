#!/usr/bin/env python3
import pandas as pd
import numpy as np
import json
import os

def comprehensive_analysis():
    """수집된 데이터로 종합 분석"""
    print("="*50)
    print("COMPREHENSIVE YOUTUBE DATA ANALYSIS")
    print("="*50)
    
    # 데이터 로드
    df = pd.read_csv('data/processed/kaggle_youtube_cleaned.csv')
    
    # 1. 기본 통계
    print("\n📊 BASIC STATISTICS")
    print("-"*30)
    print(f"Total videos: {len(df):,}")
    print(f"Unique videos: {df['video_id'].nunique():,}")
    print(f"Date range: {df['trending_date'].min()} to {df['trending_date'].max()}")
    print(f"Countries: {df['source_file'].nunique()}")
    
    # 2. 조회수 분석
    print("\n👁️ VIEW STATISTICS")
    print("-"*30)
    print(f"Total views: {df['views'].sum():,}")
    print(f"Average views: {df['views'].mean():,.0f}")
    print(f"Median views: {df['views'].median():,.0f}")
    print(f"Max views: {df['views'].max():,}")
    
    # 3. 참여율 분석
    print("\n💬 ENGAGEMENT ANALYSIS")
    print("-"*30)
    print(f"Average engagement rate: {df['engagement_rate'].mean():.4f}")
    print(f"Average like ratio: {df['like_ratio'].mean():.4f}")
    
    # 4. 카테고리 분석
    print("\n🏷️ TOP CATEGORIES")
    print("-"*30)
    top_categories = df['category_name'].value_counts().head(5)
    for cat, count in top_categories.items():
        pct = (count / len(df)) * 100
        print(f"{cat}: {count:,} ({pct:.1f}%)")
    
    # 5. 국가별 분석
    print("\n🌍 COUNTRY COMPARISON")
    print("-"*30)
    country_stats = df.groupby('source_file').agg({
        'views': 'mean',
        'engagement_rate': 'mean',
        'video_id': 'count'
    }).round(2)
    country_stats.columns = ['Avg Views', 'Avg Engagement', 'Video Count']
    country_stats = country_stats.sort_values('Avg Views', ascending=False)
    print(country_stats.head())
    
    # 6. 시간대별 트렌드
    df['trending_date'] = pd.to_datetime(df['trending_date'])
    df['year_month'] = df['trending_date'].dt.to_period('M')
    
    print("\n📈 TEMPORAL TRENDS")
    print("-"*30)
    monthly_videos = df.groupby('year_month').size()
    print(f"Videos per month (avg): {monthly_videos.mean():.0f}")
    print(f"Peak month: {monthly_videos.idxmax()} ({monthly_videos.max()} videos)")
    
    # 7. 상관관계 분석
    print("\n🔗 CORRELATIONS")
    print("-"*30)
    numeric_cols = ['views', 'likes', 'dislikes', 'comment_count', 'engagement_rate']
    existing_cols = [col for col in numeric_cols if col in df.columns]
    correlations = df[existing_cols].corr()
    
    # 가장 강한 상관관계
    mask = np.triu(np.ones_like(correlations), k=1)
    correlations_masked = correlations.mask(mask)
    max_corr = correlations_masked.unstack().sort_values(ascending=False).head(3)
    
    for (col1, col2), corr in max_corr.items():
        print(f"{col1} ↔ {col2}: {corr:.3f}")
    
    # 결과 저장
    results = {
        'total_videos': int(len(df)),
        'unique_videos': int(df['video_id'].nunique()),
        'countries': int(df['source_file'].nunique()),
        'avg_views': float(df['views'].mean()),
        'avg_engagement': float(df['engagement_rate'].mean()),
        'top_category': str(df['category_name'].mode()[0]),
        'analysis_date': pd.Timestamp.now().isoformat()
    }
    
    with open('data/processed/analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n" + "="*50)
    print("✅ ANALYSIS COMPLETE!")
    print("Results saved to data/processed/analysis_results.json")
    print("="*50)
    
    return results

if __name__ == "__main__":
    # 종합 분석 실행
    results = comprehensive_analysis()
    
    print("\n🎯 KEY INSIGHTS:")
    print(f"1. The dataset contains {results['total_videos']:,} trending videos")
    print(f"2. Average view count is {results['avg_views']:,.0f}")
    print(f"3. Most popular category: {results['top_category']}")
    print(f"4. Data covers {results['countries']} different countries")
