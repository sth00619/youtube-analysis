import requests
import json
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

class ArchiveFeasibleCollector:
    def __init__(self):
        self.base_url = "https://archive.org"
        self.output_dir = "data/raw/internet_archive"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Cloud Shell에서 다운로드 가능한 작은 데이터셋만
        self.target_datasets = [
            {
                'identifier': 'youtube-dataset-2019',
                'max_size_mb': 100
            },
            {
                'identifier': 'youtube_trending_data',
                'max_size_mb': 50
            }
        ]
    
    def search_small_datasets(self):
        """작은 YouTube 데이터셋 검색 (100MB 이하)"""
        search_url = f"{self.base_url}/advancedsearch.php"
        
        # 작은 파일만 검색
        params = {
            'q': 'youtube AND mediatype:data AND item_size:[0 TO 100000000]',  # 100MB 이하
            'fl': 'identifier,title,size,format,downloads',
            'rows': 50,
            'output': 'json',
            'sort': '-downloads'
        }
        
        print("Searching for small YouTube datasets (<100MB)...")
        
        try:
            response = requests.get(search_url, params=params)
            if response.status_code == 200:
                data = response.json()
                items = data.get('response', {}).get('docs', [])
                
                results = []
                for item in items:
                    size_bytes = int(item.get('size', 0))
                    size_mb = size_bytes / (1024*1024)
                    
                    if size_mb > 0 and size_mb < 100:
                        results.append({
                            'identifier': item['identifier'],
                            'title': item.get('title', 'N/A'),
                            'size_mb': size_mb,
                            'downloads': item.get('downloads', 0)
                        })
                
                # 다운로드 수 기준 정렬
                results.sort(key=lambda x: x['downloads'], reverse=True)
                
                print(f"\nFound {len(results)} downloadable datasets:")
                for r in results[:5]:
                    print(f"  - {r['identifier']}: {r['size_mb']:.1f}MB ({r['downloads']} downloads)")
                
                return results[:5]  # 상위 5개만
                
        except Exception as e:
            print(f"Search failed: {e}")
            return []
    
    def download_dataset(self, identifier, max_size_mb=100):
        """특정 데이터셋 다운로드"""
        metadata_url = f"{self.base_url}/metadata/{identifier}"
        
        print(f"\nFetching metadata for: {identifier}")
        
        try:
            response = requests.get(metadata_url)
            if response.status_code == 200:
                metadata = response.json()
                
                # CSV, JSON, TXT 파일 찾기
                files = metadata.get('files', [])
                downloaded_files = []
                
                for file_info in files:
                    filename = file_info.get('name', '')
                    size = int(file_info.get('size', 0))
                    size_mb = size / (1024*1024)
                    
                    # 작은 데이터 파일만
                    if size_mb < max_size_mb and any(ext in filename.lower() for ext in ['.csv', '.json', '.txt', '.tsv']):
                        print(f"  Downloading {filename} ({size_mb:.1f}MB)...")
                        
                        download_url = f"{self.base_url}/download/{identifier}/{filename}"
                        output_path = os.path.join(self.output_dir, f"{identifier}_{filename}")
                        
                        response = requests.get(download_url, stream=True)
                        
                        with open(output_path, 'wb') as f:
                            with tqdm(total=size, unit='B', unit_scale=True, desc=filename) as pbar:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                                    pbar.update(len(chunk))
                        
                        downloaded_files.append(output_path)
                        print(f"  ✅ Saved: {output_path}")
                        
                        # 파일 분석
                        self.analyze_file(output_path)
                
                return downloaded_files
                
        except Exception as e:
            print(f"Download failed: {e}")
            return []
    
    def analyze_file(self, filepath):
        """다운로드한 파일 분석"""
        try:
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath, nrows=1000)
                print(f"    Shape: {df.shape}")
                print(f"    Columns: {list(df.columns)[:5]}")
            elif filepath.endswith('.json'):
                with open(filepath, 'r') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    print(f"    JSON array with {len(data)} items")
                elif isinstance(data, dict):
                    print(f"    JSON object with keys: {list(data.keys())[:5]}")
        except Exception as e:
            print(f"    Analysis failed: {e}")
    
    def collect_all(self):
        """모든 수집 작업 실행"""
        print("="*50)
        print("Internet Archive Collection (Feasible)")
        print("="*50)
        
        # 1. 작은 데이터셋 검색
        datasets = self.search_small_datasets()
        
        # 2. 상위 3개 다운로드
        all_files = []
        for dataset in datasets[:3]:
            files = self.download_dataset(dataset['identifier'], max_size_mb=50)
            all_files.extend(files)
        
        # 3. 요약
        print("\n=== Collection Summary ===")
        print(f"Total files downloaded: {len(all_files)}")
        
        total_size = sum(os.path.getsize(f) for f in all_files if os.path.exists(f))
        print(f"Total size: {total_size/(1024*1024):.1f}MB")
        
        # 요약 저장
        summary = {
            'datasets': datasets[:3],
            'files': all_files,
            'total_size_mb': total_size/(1024*1024),
            'timestamp': datetime.now().isoformat()
        }
        
        summary_file = os.path.join(self.output_dir, 'collection_summary.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        return all_files

if __name__ == "__main__":
    collector = ArchiveFeasibleCollector()
    files = collector.collect_all()