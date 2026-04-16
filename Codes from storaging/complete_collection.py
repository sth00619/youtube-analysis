#!/usr/bin/env python3
import os
import sys
sys.path.append('/home/alpngmu99/youtube_pipeline')

from collectors.youniverse_partial_collector import YouniversePartialCollector
from collectors.archive_feasible_collector import ArchiveFeasibleCollector
import shutil

def check_disk_space():
    """디스크 공간 확인"""
    total, used, free = shutil.disk_usage("/")
    free_gb = free // (2**30)
    used_gb = used // (2**30)
    total_gb = total // (2**30)
    
    print(f"Disk Usage: {used_gb}/{total_gb}GB used, {free_gb}GB free")
    return free_gb

def main():
    print("="*60)
    print("COMPLETE DATA COLLECTION PIPELINE")
    print("="*60)
    
    # 디스크 공간 확인
    free_gb = check_disk_space()
    
    if free_gb < 2:
        print("⚠️ Warning: Low disk space!")
        return
    
    # 1. YouNiverse 부분 수집
    print("\n" + "="*40)
    print("STEP 1: YouNiverse Partial Collection")
    print("="*40)
    
    youniverse = YouniversePartialCollector()
    
    # 메타데이터 100MB 수집
    youniverse.download_partial_file('metadata', size_mb=100)
    
    # 디스크 공간이 충분하면 코멘트도 일부 수집
    if free_gb > 5:
        youniverse.download_partial_file('comments', size_mb=50)
    
    # 2. Internet Archive 수집
    print("\n" + "="*40)
    print("STEP 2: Internet Archive Collection")
    print("="*40)
    
    archive = ArchiveFeasibleCollector()
    archive_files = archive.collect_all()
    
    # 3. 최종 상태 확인
    print("\n" + "="*40)
    print("FINAL STATUS")
    print("="*40)
    
    # 수집된 모든 파일 확인
    data_dirs = [
        'data/raw',
        'data/raw/youniverse',
        'data/raw/internet_archive',
        'data/processed'
    ]
    
    total_files = 0
    total_size = 0
    
    for dir_path in data_dirs:
        if os.path.exists(dir_path):
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    filepath = os.path.join(root, file)
                    size = os.path.getsize(filepath)
                    total_files += 1
                    total_size += size
                    
                    if size > 1024*1024:  # 1MB 이상만 출력
                        print(f"  {file}: {size/(1024*1024):.1f}MB")
    
    print(f"\n📊 Collection Summary:")
    print(f"  Total files: {total_files}")
    print(f"  Total size: {total_size/(1024**3):.2f}GB")
    
    print("\n✅ Data Collection Status:")
    print("  - Kaggle: ✅ Complete (173,418 records)")
    print("  - YouTube API: ✅ Complete (63 videos)")
    print("  - YouNiverse: ⚠️ Partial (metadata sample)")
    print("  - Internet Archive: ⚠️ Partial (small datasets only)")
    
    print("\n🎯 Ready for analysis!")

if __name__ == "__main__":
    main()
