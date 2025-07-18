import sqlite3
import pandas as pd
from sqlalchemy import create_engine
class DataLoader:
    def __init__(self, db_path):
        self.db_path = db_path

    def load_data(self, table_name, nrow):
        try:
            eng = create_engine(f"sqlite:///{self.db_path}")
            with eng.connect()as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(sql = query, con = conn)
        except Exception as e :
            print(f"{table_name}에서 {e} 발생")
            df = pd.DataFrame()
        finally :
            conn.close()
        return df
    

    def save_data(self, data, table_name) :
        '''
        데이트 프레임을 db의 테이블에 저장해주는 함수
        '''
        try :
            conn = sqlite3.connect(self.db_path)
            data.to_sql(table_name, conn, if_exists = 'replace')
        except Exception as e :
            print(f"{e}")
        finally :
            conn.close()


    def get_table_names(self):
        try :
            conn = sqlite3.connect(self.db_path)
            query = "SELECT name from sqlite_master WHERE type = 'table'"
            tables = pd.read_sql(query, conn)
        except Exception as e :
            print(e)
            tables = pd.DataFrame()
        finally : conn.close()
        return tables['name'].tolist()

    def get_table_columns(self, table_name):
        try :
            conn = sqlite3.connect(self.db_path)
            query = f"PRAGMA table_info({table_name})"
            columns = pd.read_sql(query, conn)
        except Exception as e :
            print(e)
            tables = pd.DataFrame()
        finally : conn.close()
        return columns[['name', 'type']]
    

def load_election_data(election_list, db_path, table_name, start_date=None, end_date=None):
    from datetime import datetime, timedelta
    # DB 엔진 연결
    eng = create_engine(f"sqlite:///{db_path}")
    
    #쿼리 : 
    query = f'''
    SELECT *
    FROM {table_name}
    WHERE date(년 || '-' || 
               CASE WHEN length(월) = 1 THEN '0' || 월 ELSE 월 END || '-' || 
               CASE WHEN length(일) = 1 THEN '0' || 일 ELSE 일 END) 
    BETWEEN date(?) AND date(?)
    ORDER BY 년, 월, 일
    '''
    
    election_dataframes = {}
    with eng.connect() as conn:
        for election_name, election_date in election_list.items():
            # 만약 start_date와 end_date가 None이면 선거일을 기준으로 설정
            if start_date is None or end_date is None:
                # 선거일을 datetime 객체로 파싱
                calculated_end_date = datetime.strptime(election_date, '%y%m%d')
                # 선거일 1년 전 계산
                calculated_start_date = calculated_end_date - timedelta(days=365)
                
                # 날짜를 문자열로 변환
                start_date_str = calculated_start_date.strftime('%Y-%m-%d')
                end_date_str = calculated_end_date.strftime('%Y-%m-%d')
            else:
                # 입력된 start_date와 end_date가 문자열인지 확인하고 변환
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, '%y%m%d')
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, '%y%m%d')
                
                # datetime 객체로 변환 후 문자열로 변환
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = end_date.strftime('%Y-%m-%d')
            
            # 데이터 불러오기
            df = pd.read_sql_query(query, conn, params=(start_date_str, end_date_str))
            
            # 결과 저장
            election_dataframes[election_name] = df
            
            print(f"Loaded {df.shape[0]} rows for {election_name}: {start_date_str} to {end_date_str}")
            
    return election_dataframes

# db_path = 'data/raw/RealEstate.db'
# loader = DataLoader(db_path)
# loader.load_data('apt_raw')