#!/usr/bin/env python3
import os
import sys
import time
import shutil
import json
sys.path.append('/home/alpngmu99/youtube_pipeline')

from cleaners.data_cleaner import YouTubeDataCleaner
from collectors.youtube_collector import YouTubeCollector
from collectors.youniverse_working_collector import YouniverseWorkingCollector
from collectors.archive_feasible_collector import ArchiveFeasibleCollector
from config.api_keys import YOUTUBE_API_KEYS
import pandas as pd

def print_section(title):
    """섹션 구분선 출력"""
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)

def check_system_status():
    """시스템 상태 확인"""
    print_section("SYSTEM STATUS CHECK")
    
    # 디스크 공간
    total, used, free = shutil.disk_usage("/")
    free_gb = free // (2**30)
    
    print(f"💾 Disk Space: {free_gb}GB available")
    
    if free_gb < 2:
        print("⚠️  Warning: Low disk space!")
        return False
    
    # 디렉토리 생성
    dirs = ['data/raw', 'data/processed', 'data/raw/youniverse', 'data/raw/internet_archive']
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
    
    print("✅ All directories ready")
    return True

def step1_kaggle_processing():
    """Step 1: Kaggle 데이터 처리"""
    print_section("STEP 1: KAGGLE DATA PROCESSING")
    
    cleaned_file = 'data/processed/kaggle_youtube_cleaned.csv'
    
    if os.path.exists(cleaned_file):
        print("✅ Kaggle data already processed")
        df = pd.read_csv(cleaned_file)
        print(f"   Records: {len(df):,}")
        print(f"   Countries: {df['source_file'].nunique()}")
        return True
    else:
        print("Processing Kaggle data...")
        cleaner = YouTubeDataCleaner()
        kaggle_df = cleaner.load_kaggle_data('/home/alpngmu99/youtube_pipeline/kaggle_data')
        
        if not kaggle_df.empty:
            cleaned_df = cleaner.clean_youtube_data(kaggle_df)
            cleaner.save_cleaned_data(cleaned_df, 'kaggle_youtube_cleaned')
            return True
        return False

def step2_youtube_api():
    """Step 2: YouTube API 데이터 수집"""
    print_section("STEP 2: YOUTUBE API COLLECTION")
    
    # API 키 확인
    if not YOUTUBE_API_KEYS or YOUTUBE_API_KEYS[0].startswith('YOUR_'):
        print("⚠️  No valid API key configured")
        return False
    
    try:
        # 이미 수집된 파일 확인
        api_files = [f for f in os.listdir('data/raw') if 'youtube_api' in f]
        if api_files:
            print(f"✅ API data already collected: {len(api_files)} files")
            return True
        
        # 새로 수집
        cleaned_df = pd.read_csv('data/processed/kaggle_youtube_cleaned.csv')
        video_ids = cleaned_df['video_id'].dropna().unique()[:100]
        
        print(f"Collecting data for {len(video_ids)} videos...")
        collector = YouTubeCollector(YOUTUBE_API_KEYS)
        results = collector.collect_videos_batch(video_ids)
        
        if results:
            collector.save_results(results, 'youtube_api_data')
            print(f"✅ Collected {len(results)} videos")
            return True
            
    except Exception as e:
        print(f"❌ API collection failed: {e}")
    
    return False

def step3_youniverse():
    """Step 3: YouNiverse 데이터 수집"""
    print_section("STEP 3: YOUNIVERSE COLLECTION")
    
    try:
        collector = YouniverseWorkingCollector()
        
        # 메타데이터
        print("\n📋 Getting metadata...")
        collector.get_metadata()
        
        # 작은 파일들 다운로드
        print("\n📥 Downloading small files...")
        collector.download_small_files()
        
        # 큰 파일 샘플
        print("\n📥 Downloading sample of large file...")
        collector.download_partial('yt_metadata_en', size_mb=100)
        
        print("✅ YouNiverse collection complete")
        return True
        
    except Exception as e:
        print(f"⚠️  YouNiverse partial failure: {e}")
        return False

def step4_internet_archive():
    """Step 4: Internet Archive 데이터 수집"""
    print_section("STEP 4: INTERNET ARCHIVE COLLECTION")
    
    try:
        collector = ArchiveFeasibleCollector()
        files = collector.collect_all()
        
        if files:
            print(f"✅ Downloaded {len(files)} files from Internet Archive")
            return True
        else:
            print("⚠️  No files downloaded from Internet Archive")
            
    except Exception as e:
        print(f"❌ Internet Archive collection failed: {e}")
    
    return False

def step5_final_summary():
    """Step 5: 최종 요약 및 분석"""
    print_section("STEP 5: FINAL SUMMARY & ANALYSIS")
    
    summary = {
        'timestamp': pd.Timestamp.now().isoformat(),
        'status': {},
        'statistics': {}
    }
    
    # 1. Kaggle 데이터 확인
    kaggle_file = 'data/processed/kaggle_youtube_cleaned.csv'
    if os.path.exists(kaggle_file):
        df = pd.read_csv(kaggle_file)
        summary['status']['kaggle'] = '✅ Complete'
        summary['statistics']['kaggle'] = {
            'records': len(df),
            'unique_videos': df['video_id'].nunique(),
            'countries': df['source_file'].nunique(),
            'avg_views': float(df['views'].mean()),
            'size_mb': os.path.getsize(kaggle_file) / (1024*1024)
        }
        
        print(f"\n📊 Kaggle Data:")
        print(f"   Records: {len(df):,}")
        print(f"   Unique videos: {df['video_id'].nunique():,}")
        print(f"   Average views: {df['views'].mean():,.0f}")
    
    # 2. API 데이터 확인
    api_files = [f for f in os.listdir('data/raw') if 'youtube_api' in f and f.endswith('.json')]
    if api_files:
        summary['status']['youtube_api'] = '✅ Complete'
        total_api_videos = 0
        for file in api_files:
            with open(f'data/raw/{file}', 'r') as f:
                data = json.load(f)
                total_api_videos += len(data)
        summary['statistics']['youtube_api'] = {'videos': total_api_videos}
        print(f"\n📊 YouTube API:")
        print(f"   Videos enriched: {total_api_videos}")
    
    # 3. YouNiverse 데이터 확인
    youniverse_files = []
    if os.path.exists('data/raw/youniverse'):
        youniverse_files = os.listdir('data/raw/youniverse')
        if youniverse_files:
            summary['status']['youniverse'] = '⚠️ Partial'
            total_size = sum(os.path.getsize(os.path.join('data/raw/youniverse', f)) 
                           for f in youniverse_files) / (1024*1024)
            summary['statistics']['youniverse'] = {
                'files': len(youniverse_files),
                'size_mb': total_size
            }
            print(f"\n📊 YouNiverse:")
            print(f"   Files: {len(youniverse_files)}")
            print(f"   Size: {total_size:.1f}MB")
    
    # 4. Internet Archive 데이터 확인
    if os.path.exists('data/raw/internet_archive'):
        archive_files = os.listdir('data/raw/internet_archive')
        if archive_files:
            summary['status']['internet_archive'] = '⚠️ Partial'
            summary['statistics']['internet_archive'] = {'files': len(archive_files)}
            print(f"\n📊 Internet Archive:")
            print(f"   Files: {len(archive_files)}")
    
    # 5. 전체 디스크 사용량
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk('data'):
        for file in files:
            filepath = os.path.join(root, file)
            total_size += os.path.getsize(filepath)
            file_count += 1
    
    summary['statistics']['total'] = {
        'files': file_count,
        'size_gb': total_size / (1024**3)
    }
    
    print(f"\n📊 Total Data:")
    print(f"   Files: {file_count}")
    print(f"   Size: {total_size/(1024**3):.2f}GB")
    
    # 요약 저장
    with open('data/collection_summary_final.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    return summary

def main():
    """메인 파이프라인 실행"""
    print("="*60)
    print(" COMPLETE YOUTUBE DATA PIPELINE")
    print("="*60)
    print(f" Start Time: {pd.Timestamp.now()}")
    print("="*60)
    
    start_time = time.time()
    
    # 시스템 체크
    if not check_system_status():
        print("❌ System check failed. Exiting.")
        return
    
    # 각 단계 실행
    steps = [
        ("Kaggle Processing", step1_kaggle_processing),
        ("YouTube API", step2_youtube_api),
        ("YouNiverse", step3_youniverse),
        ("Internet Archive", step4_internet_archive)
    ]
    
    results = {}
    for name, func in steps:
        try:
            results[name] = func()
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            results[name] = False
    
    # 최종 요약
    summary = step5_final_summary()
    
    # 실행 시간
    elapsed = time.time() - start_time
    
    print_section("PIPELINE COMPLETE")
    print(f"⏱️  Total time: {elapsed:.1f} seconds")
    print("\n📊 Final Status:")
    for step, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {step}")
    
    print("\n🎯 Data Collection Summary:")
    print(f"   Kaggle: {summary.get('status', {}).get('kaggle', '❌')}")
    print(f"   YouTube API: {summary.get('status', {}).get('youtube_api', '❌')}")
    print(f"   YouNiverse: {summary.get('status', {}).get('youniverse', '❌')}")
    print(f"   Internet Archive: {summary.get('status', {}).get('internet_archive', '❌')}")
    
    print("\n✅ Pipeline execution complete!")
    print(f"📁 Summary saved to: data/collection_summary_final.json")

if __name__ == "__main__":
    main()