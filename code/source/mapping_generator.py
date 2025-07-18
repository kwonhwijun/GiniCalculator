import pandas as pd
import sqlite3
from datetime import datetime
import logging
import os

class MappingGenerator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        logging.basicConfig(level=logging.INFO)
        
    def create_mapping_tables(self):
        """기본 매핑 테이블 생성"""
        with self.conn:
            # 법정동-선거구 매핑 테이블
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS legal_district_mapping (
                    legal_code TEXT,
                    legal_name TEXT,
                    sido TEXT,
                    sigungu TEXT,
                    eupmyeondong TEXT,
                    district_code TEXT,
                    district_name TEXT,
                    valid_from DATE,
                    valid_to DATE,
                    election_round TEXT,
                    notes TEXT,
                    PRIMARY KEY (legal_code, district_code, valid_from)
                )
            ''')
            
            # 변경 이력 테이블
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS mapping_history (
                    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    legal_code TEXT,
                    district_code TEXT,
                    change_type TEXT,
                    change_date DATE,
                    old_value TEXT,
                    new_value TEXT,
                    reason TEXT
                )
            ''')
    
    def import_legal_codes(self, legal_code_dir):
        """법정동코드 데이터 임포트"""
        # 모든 법정동코드 파일 읽기
        all_data = []
        for file in os.listdir(legal_code_dir):
            if file.startswith("법정동코드 조회자료") and file.endswith(".xls"):
                file_path = os.path.join(legal_code_dir, file)
                df = pd.read_excel(file_path)
                all_data.append(df)
        
        # 데이터 합치기
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # 중복 제거
        combined_df = combined_df.drop_duplicates()
        
        # 날짜 형식 변환
        combined_df['valid_from'] = pd.to_datetime(combined_df['생성일자'])
        combined_df['valid_to'] = pd.to_datetime(combined_df['말소일자'])
        
        # 필요한 컬럼만 선택하고 이름 변경
        mapping_df = combined_df[[
            '법정동코드', '법정동명', '시도명', '시군구명', '읍면동명',
            'valid_from', 'valid_to'
        ]].rename(columns={
            '법정동코드': 'legal_code',
            '법정동명': 'legal_name',
            '시도명': 'sido',
            '시군구명': 'sigungu',
            '읍면동명': 'eupmyeondong'
        })
        
        return mapping_df
    
    def import_district_codes(self, district_dir, election_round):
        """선거구 데이터 임포트"""
        # hwp 파일을 엑셀로 변환하는 작업이 필요합니다.
        # 현재는 수동으로 변환된 엑셀 파일을 사용한다고 가정
        district_file = os.path.join(district_dir, f"{election_round}_선거구_행정동_매칭.xlsx")
        
        if not os.path.exists(district_file):
            logging.error(f"선거구 매칭 파일이 없습니다: {district_file}")
            return None
            
        df = pd.read_excel(district_file)
        df['election_round'] = election_round
        return df
    
    def generate_mapping(self, legal_df, district_df):
        """법정동-선거구 매핑 생성"""
        if district_df is None:
            return None
            
        # 여기에 매핑 로직 구현
        merged_df = pd.merge(
            legal_df,
            district_df,
            how='left',
            left_on=['sido', 'sigungu', 'eupmyeondong'],
            right_on=['시도명', '시군구명', '읍면동명']
        )
        
        return merged_df
    
    def save_mapping(self, mapping_df):
        """생성된 매핑을 DB에 저장"""
        if mapping_df is not None:
            mapping_df.to_sql(
                'legal_district_mapping',
                self.conn,
                if_exists='append',
                index=False
            )
    
    def export_mapping(self, output_file):
        """현재 유효한 매핑을 엑셀 파일로 추출"""
        query = '''
        SELECT * FROM legal_district_mapping
        WHERE valid_to IS NULL
        OR valid_to > date('now')
        '''
        
        df = pd.read_sql_query(query, self.conn)
        df.to_excel(output_file, index=False)
        
    def close(self):
        """DB 연결 종료"""
        self.conn.close()

# 사용 예시
if __name__ == "__main__":
    # 기본 경로 설정
    legal_code_dir = "code/data/raw/법정동코드_변경내역"
    district_dir = "data/processed/선거구수기2"
    
    # 매핑 생성기 초기화
    generator = MappingGenerator("mapping.db")
    generator.create_mapping_tables()
    
    # 법정동코드 데이터 임포트
    logging.info("법정동코드 데이터 임포트 중...")
    legal_df = generator.import_legal_codes(legal_code_dir)
    
    # 각 선거별로 처리
    election_rounds = ["18대_국회의원", "19대_국회의원", "20대_국회의원", "21대_국회의원", "22대_국회의원"]
    
    for election_round in election_rounds:
        logging.info(f"{election_round} 처리 중...")
        
        # 선거구 데이터 임포트
        district_df = generator.import_district_codes(district_dir, election_round)
        
        # 매핑 생성
        mapping_df = generator.generate_mapping(legal_df, district_df)
        
        # DB에 저장
        generator.save_mapping(mapping_df)
    
    # 최종 매핑 파일 추출
    generator.export_mapping("최종_매핑.xlsx")
    
    generator.close()
    logging.info("처리 완료") 