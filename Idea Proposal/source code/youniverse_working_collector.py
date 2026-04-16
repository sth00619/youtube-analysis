import requests
import os
import gzip
import pandas as pd
from tqdm import tqdm
import json

class YouniverseWorkingCollector:
    def __init__(self):
        # Zenodo에서 확인된 실제 파일들
        self.record_id = "4650046"
        self.base_url = f"https://zenodo.org/api/records/{self.record_id}"
        
        # 페이지에서 확인된 파일들 (크기 순)
        self.files = {
            'df_channels_en': {
                'filename': 'df_channels_en.tsv.gz',
                'size_mb': 6.0,
                'description': 'English YouTube channels metadata'
            },
            'df_timeseries_en': {
                'filename': 'df_timeseries_en.tsv.gz', 
                'size_mb': 571.1,
                'description': 'Time series data'
            },
            'num_comments': {
                'filename': 'num_comments.tsv.gz',
                'size_mb': 754.6,
                'description': 'Number of comments per video'
            },
            'yt_metadata_en': {
                'filename': 'yt_metadata_en.jsonl.gz',
                'size_mb': 14700,  # 14.7GB
                'description': 'Video metadata in JSON format'
            },
            'youtube_comments': {
                'filename': 'youtube_comments.tsv.gz',
                'size_mb': 77200,  # 77.2GB
                'description': 'YouTube comments data'
            }
        }
        
        self.output_dir = "data/raw/youniverse"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_download_url(self, filename):
        """실제 다운로드 URL 생성"""
        # Zenodo 다운로드 URL 형식
        return f"https://zenodo.org/records/{self.record_id}/files/{filename}?download=1"
    
    def download_small_files(self):
        """작은 파일들 전체 다운로드 (1GB 미만)"""
        small_files = ['df_channels_en', 'df_timeseries_en', 'num_comments']
        
        for file_key in small_files:
            file_info = self.files[file_key]
            filename = file_info['filename']
            size_mb = file_info['size_mb']
            
            if size_mb < 1000:  # 1GB 미만만
                print(f"\n📥 Downloading {filename} ({size_mb}MB)...")
                print(f"   Description: {file_info['description']}")
                
                url = self.get_download_url(filename)
                output_path = os.path.join(self.output_dir, filename)
                
                try:
                    response = requests.get(url, stream=True)
                    response.raise_for_status()
                    
                    total_size = int(response.headers.get('content-length', 0))
                    
                    with open(output_path, 'wb') as f:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename) as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                                pbar.update(len(chunk))
                    
                    print(f"✅ Downloaded: {output_path}")
                    
                    # 압축 해제 시도
                    self.extract_and_analyze(output_path)
                    
                except Exception as e:
                    print(f"❌ Failed to download {filename}: {e}")
    
    def download_partial(self, file_key='yt_metadata_en', size_mb=100):
        """큰 파일의 일부만 다운로드"""
        if file_key not in self.files:
            print(f"Invalid file key. Available: {list(self.files.keys())}")
            return None
        
        file_info = self.files[file_key]
        filename = file_info['filename']
        
        print(f"\n📥 Downloading {size_mb}MB sample of {filename}")
        print(f"   Full size: {file_info['size_mb']}MB")
        print(f"   Description: {file_info['description']}")
        
        url = self.get_download_url(filename)
        output_path = os.path.join(self.output_dir, f"{filename.split('.')[0]}_{size_mb}mb_sample.gz")
        
        size_bytes = size_mb * 1024 * 1024
        
        try:
            # Range header로 부분 다운로드
            headers = {'Range': f'bytes=0-{size_bytes-1}'}
            response = requests.get(url, headers=headers, stream=True)
            
            if response.status_code in [200, 206]:
                with open(output_path, 'wb') as f:
                    downloaded = 0
                    with tqdm(total=size_bytes, unit='B', unit_scale=True) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if downloaded >= size_bytes:
                                break
                            f.write(chunk)
                            downloaded += len(chunk)
                            pbar.update(len(chunk))
                
                print(f"✅ Partial download saved: {output_path}")
                return output_path
            else:
                print(f"❌ Server returned status {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Download error: {e}")
            return None
    
    def extract_and_analyze(self, gz_file):
        """압축 파일 해제 및 분석"""
        try:
            # 파일 확장자 확인
            if '.jsonl.gz' in gz_file:
                output_file = gz_file.replace('.gz', '')
                print(f"Extracting JSONL file...")
                
                with gzip.open(gz_file, 'rt', encoding='utf-8', errors='ignore') as f_in:
                    lines = []
                    for i, line in enumerate(f_in):
                        if i >= 1000:  # 처음 1000줄만
                            break
                        lines.append(json.loads(line))
                
                print(f"  Extracted {len(lines)} JSON objects")
                
                # 샘플 저장
                sample_file = output_file.replace('.jsonl', '_sample.json')
                with open(sample_file, 'w') as f:
                    json.dump(lines[:10], f, indent=2)
                print(f"  Sample saved: {sample_file}")
                
            elif '.tsv.gz' in gz_file:
                output_file = gz_file.replace('.gz', '')
                print(f"Extracting TSV file...")
                
                # 처음 몇 줄만 추출
                with gzip.open(gz_file, 'rt', encoding='utf-8', errors='ignore') as f_in:
                    lines = []
                    for i, line in enumerate(f_in):
                        if i >= 10000:  # 처음 10000줄
                            break
                        lines.append(line)
                
                sample_file = output_file.replace('.tsv', '_sample.tsv')
                with open(sample_file, 'w') as f:
                    f.writelines(lines)
                
                print(f"  Extracted {len(lines)} lines to {sample_file}")
                
                # Pandas로 분석
                df = pd.read_csv(sample_file, sep='\t', nrows=1000, on_bad_lines='skip')
                print(f"  Data shape: {df.shape}")
                print(f"  Columns: {list(df.columns)[:5]}...")
                
                # CSV로 변환
                csv_file = sample_file.replace('.tsv', '.csv')
                df.to_csv(csv_file, index=False)
                print(f"  Converted to CSV: {csv_file}")
                
        except Exception as e:
            print(f"  Extraction/analysis failed: {e}")
    
    def get_metadata(self):
        """Zenodo API로 메타데이터 가져오기"""
        print("\n📋 Fetching dataset metadata...")
        
        try:
            response = requests.get(self.base_url)
            if response.status_code == 200:
                metadata = response.json()
                
                # 메타데이터 저장
                metadata_file = os.path.join(self.output_dir, 'dataset_metadata.json')
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                print(f"✅ Metadata saved: {metadata_file}")
                
                # 주요 정보 출력
                print("\n=== Dataset Information ===")
                print(f"Title: {metadata['metadata']['title']}")
                print(f"DOI: {metadata['doi']}")
                print(f"Publication date: {metadata['created']}")
                print(f"Size: {metadata['metadata'].get('size', 'N/A')}")
                
                return metadata
                
        except Exception as e:
            print(f"❌ Failed to fetch metadata: {e}")
            return None

if __name__ == "__main__":
    collector = YouniverseWorkingCollector()
    
    print("="*50)
    print("YouNiverse Dataset Collection")
    print("="*50)
    
    # 1. 메타데이터 가져오기
    collector.get_metadata()
    
    # 2. 작은 파일들 다운로드 (df_channels_en만 - 6MB)
    print("\n[Step 1] Downloading small files...")
    collector.download_small_files()
    
    # 3. 큰 파일 샘플 다운로드
    print("\n[Step 2] Downloading samples of large files...")
    collector.download_partial('yt_metadata_en', size_mb=100)
    
    print("\n✅ Collection complete!")