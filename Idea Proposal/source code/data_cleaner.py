import pandas as pd
import numpy as np
import json
import isodate
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

class YouTubeDataCleaner:
    def __init__(self):
        self.category_mapping = {
            1: 'Film & Animation', 2: 'Autos & Vehicles',
            10: 'Music', 15: 'Pets & Animals',
            17: 'Sports', 19: 'Travel & Events',
            20: 'Gaming', 22: 'People & Blogs',
            23: 'Comedy', 24: 'Entertainment',
            25: 'News & Politics', 26: 'Howto & Style',
            27: 'Education', 28: 'Science & Technology'
        }
        
    def load_kaggle_data(self, kaggle_data_path):
        """Kaggle 데이터 로드 (인코딩 문제 해결)"""
        print(f"Loading Kaggle data from {kaggle_data_path}")
        
        # CSV 파일들 찾기
        csv_files = [f for f in os.listdir(kaggle_data_path) if f.endswith('.csv')]
        
        dfs = []
        for file in csv_files:
            file_path = os.path.join(kaggle_data_path, file)
            
            # 여러 인코딩 시도
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
                    print(f"Loaded {file} with {encoding} encoding: {len(df)} rows")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"Error reading {file} with {encoding}: {e}")
                    continue
            
            if df is not None:
                df['source_file'] = file
                dfs.append(df)
            else:
                print(f"Failed to load {file} with any encoding")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            print(f"Total rows loaded: {len(combined_df)}")
            return combined_df
        else:
            print("No CSV files found or loaded")
            return pd.DataFrame()
    
    def parse_duration(self, duration_str):
        """ISO 8601 duration을 초로 변환"""
        try:
            if pd.isna(duration_str) or duration_str == 'P0D':
                return 0
            duration = isodate.parse_duration(duration_str)
            return duration.total_seconds()
        except:
            return 0
    
    def clean_youtube_data(self, df):
        """YouTube 데이터 클리닝 파이프라인"""
        print("Starting data cleaning pipeline...")
        initial_rows = len(df)
        
        # 1. 중복 제거
        if 'video_id' in df.columns:
            df = df.drop_duplicates(subset=['video_id'])
            print(f"Removed duplicates: {initial_rows - len(df)} rows")
        
        # 2. 날짜 파싱
        date_columns = ['trending_date', 'publish_time', 'publishedAt']
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    print(f"Could not parse date column: {col}")
        
        # 3. 숫자형 데이터 변환
        numeric_columns = ['views', 'likes', 'dislikes', 'comment_count', 
                          'view_count', 'like_count', 'dislike_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        
        # 4. Duration 파싱 (API 데이터용)
        if 'duration' in df.columns:
            df['duration_sec'] = df['duration'].apply(self.parse_duration)
        
        # 5. Missing values 처리
        if 'title' in df.columns:
            df = df.dropna(subset=['title'])
            print(f"Removed rows with missing titles: {initial_rows - len(df)} total removed")
        
        # 6. 이상치 제거 (IQR 방법)
        def remove_outliers_iqr(data, column):
            if column not in data.columns:
                return data
            
            Q1 = data[column].quantile(0.25)
            Q3 = data[column].quantile(0.75)
            IQR = Q3 - Q1
            
            # 더 관대한 기준 적용 (3 * IQR)
            lower = max(0, Q1 - 3 * IQR)  # 음수 방지
            upper = Q3 + 3 * IQR
            
            before = len(data)
            data = data[(data[column] >= lower) & (data[column] <= upper)]
            if before - len(data) > 0:
                print(f"Removed {before - len(data)} outliers from {column}")
            return data
        
        # View count 이상치 제거
        view_col = 'views' if 'views' in df.columns else 'view_count'
        if view_col in df.columns and df[view_col].sum() > 0:
            df = remove_outliers_iqr(df, view_col)
        
        # 7. 파생 특징 생성
        if 'views' in df.columns and 'likes' in df.columns:
            df['engagement_rate'] = (df['likes'] / df['views'].replace(0, 1)).fillna(0)
            df['like_ratio'] = (df['likes'] / (df['likes'] + df.get('dislikes', 0)).replace(0, 1)).fillna(0)
        
        # 8. 카테고리 매핑
        if 'category_id' in df.columns:
            df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce')
            df['category_name'] = df['category_id'].map(self.category_mapping)
            df['category_name'] = df['category_name'].fillna('Unknown')
        
        # 9. 메모리 최적화
        for col in df.columns:
            col_type = df[col].dtype
            if col_type != 'object' and col_type != 'datetime64[ns]':
                try:
                    if df[col].dtype == 'int64':
                        df[col] = pd.to_numeric(df[col], downcast='integer')
                    elif df[col].dtype == 'float64':
                        df[col] = pd.to_numeric(df[col], downcast='float')
                except:
                    pass
        
        print(f"Cleaning complete: {len(df)} rows remaining ({len(df)/initial_rows*100:.1f}%)")
        return df
    
    def save_cleaned_data(self, df, output_name):
        """클린 데이터 저장"""
        output_dir = 'data/processed'
        os.makedirs(output_dir, exist_ok=True)
        
        # CSV 저장 (인코딩 지정)
        csv_path = f"{output_dir}/{output_name}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"Saved CSV: {csv_path}")
        
        # Parquet 저장 (압축 효율)
        try:
            parquet_path = f"{output_dir}/{output_name}.parquet"
            df.to_parquet(parquet_path, compression='snappy')
            print(f"Saved Parquet: {parquet_path}")
        except Exception as e:
            print(f"Could not save Parquet file: {e}")
        
        # 기본 통계 출력
        print("\n=== Data Summary ===")
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        print("\n=== Column Types ===")
        print(df.dtypes.value_counts())
        
        # 샘플 데이터 출력
        print("\n=== Sample Data (first 3 rows) ===")
        print(df.head(3))
        
        return csv_path

if __name__ == "__main__":
    cleaner = YouTubeDataCleaner()
    
    # Kaggle 데이터 클리닝
    kaggle_df = cleaner.load_kaggle_data('/home/alpngmu99/youtube_pipeline/kaggle_data')
    
    if not kaggle_df.empty:
        cleaned_df = cleaner.clean_youtube_data(kaggle_df)
        cleaner.save_cleaned_data(cleaned_df, 'kaggle_youtube_cleaned')
