import sqlite3
import pathlib
import pandas as pd

# --- 경로 설정 ---
# 이 스크립트 파일이 있는 폴더를 기준으로 DB 경로를 동적으로 설정합니다.
# 이렇게 하면 어떤 컴퓨터에서 실행하든 정확한 경로를 찾아줍니다.
BASE_DIR = pathlib.Path(__file__).parent
DB_PATH = BASE_DIR / "code" / "data" / "raw" / "RealEstate.db"

def print_separator(title=""):
    """구분선을 출력하는 함수"""
    if title:
        print(f"\n===== {title} =====\n")
    else:
        print("\n" + "="*30 + "\n")

def check_db_connection(db_path):
    """데이터베이스 연결을 테스트하는 함수"""
    print_separator("1. 데이터베이스 연결 테스트")
    if not db_path.exists():
        print(f"❌ 실패: 데이터베이스 파일을 찾을 수 없습니다.")
        print(f"   - 확인된 경로: {db_path}")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        print(f"✅ 성공: 데이터베이스에 성공적으로 연결되었습니다.")
        print(f"   - DB 경로: {db_path}")
        return conn
    except Exception as e:
        print(f"❌ 실패: 데이터베이스 연결 중 오류가 발생했습니다.")
        print(f"   - 오류 메시지: {e}")
        return None

def check_table_info(conn):
    """테이블 목록과 각 테이블의 데이터 개수를 확인하는 함수"""
    print_separator("2. 테이블 정보 확인")
    if not conn:
        print("   - DB 연결이 없어 테이블 정보를 확인할 수 없습니다.")
        return

    try:
        # 모든 테이블 목록 조회
        tables_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
        
        if tables_df.empty:
            print("   - 데이터베이스에 테이블이 존재하지 않습니다.")
            return

        print("   - 발견된 테이블 목록:")
        table_names = tables_df['name'].tolist()
        
        for table_name in table_names:
            # 각 테이블의 총 행 수(데이터 개수) 조회
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_df = pd.read_sql_query(count_query, conn)
            count = count_df.iloc[0, 0]
            print(f"     - [ {table_name} ]: 총 {count:,} 개의 데이터")
            
    except Exception as e:
        print(f"❌ 오류: 테이블 정보 확인 중 오류가 발생했습니다: {e}")

def check_latest_data(conn):
    """각 테이블의 최신 거래일자를 확인하는 함수"""
    print_separator("3. 최신 데이터 날짜 확인")
    if not conn:
        print("   - DB 연결이 없어 최신 데이터를 확인할 수 없습니다.")
        return

    # 확인할 테이블과 날짜 컬럼 정보
    date_columns = {
        'apt_raw': 'DEAL_YMD',      # 매매 데이터
        'apt_lease': 'DEAL_YMD'   # 전월세 데이터
    }

    try:
        for table, date_col in date_columns.items():
            # 테이블 존재 여부 먼저 확인
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if cursor.fetchone() is None:
                print(f"   - [ {table} ]: 테이블이 존재하지 않아 날짜를 확인할 수 없습니다.")
                continue

            # 최신 날짜 조회
            query = f"SELECT MAX({date_col}) FROM {table}"
            date_df = pd.read_sql_query(query, conn)
            latest_date = date_df.iloc[0, 0]
            
            if latest_date:
                print(f"   - [ {table} ] 최신 거래일자: {latest_date}")
            else:
                print(f"   - [ {table} ]: 날짜 데이터가 없습니다.")

    except Exception as e:
        print(f"❌ 오류: 최신 데이터 확인 중 오류가 발생했습니다: {e}")


def main():
    """메인 실행 함수"""
    print("데이터베이스 검증 스크립트를 시작합니다.")
    
    # 1. DB 연결 테스트
    conn = check_db_connection(DB_PATH)
    
    if conn:
        # 2. 테이블 정보 확인
        check_table_info(conn)
        
        # 3. 최신 데이터 확인 (제거됨)
        # check_latest_data(conn)
        
        # 연결 종료
        conn.close()
        print_separator()
        print("검증 완료. 데이터베이스 연결을 종료합니다.")
    else:
        print_separator()
        print("DB 연결 실패로 추가 검증을 진행할 수 없습니다.")

if __name__ == "__main__":
    main()