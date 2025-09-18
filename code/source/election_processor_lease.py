import pandas as pd
import logging
from source import calculate_gini, load_data, preprocess
import os
import PublicDataReader as pdr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_election_data(df: pd.DataFrame, region_unit: str) -> dict:
    """
    전월세 전용: 이미 로드된 DataFrame(df)과 지역 단위만 받아 지니계수 계산.
    - 시군구: 지역코드 → 시군구명/시도명 매핑 후 시도_시군구 그룹
    - 법정동: 원본 '법정동' 문자열 기준 그룹
    법정동코드/선거구 매핑 등은 수행하지 않음.
    """
    try:
        logging.info(f"[LEASE] 단일 DF 처리 시작 - 지역단위: {region_unit}")
        processor = preprocess.DataProcessor(df)
        data = processor.preprocessing()
        logging.info(f"[LEASE] 전처리 후 컬럼: {list(data.columns)}")
        logging.info(f"[LEASE] 전처리 후 샘플 지역코드: {data.get('지역코드', pd.Series(dtype=object)).astype(str).head().tolist() if '지역코드' in data.columns else '지역코드 없음'}")

        if region_unit == '시군구':
            data['지역코드'] = data['지역코드'].astype(str).str.zfill(5)
            code_bdong = pdr.code_bdong()[["시군구코드", "시군구명", "시도명"]].drop_duplicates("시군구코드")
            code_bdong.columns = code_bdong.columns.str.strip()
            code_bdong['시군구코드'] = code_bdong['시군구코드'].astype(str).str.zfill(5)
            merged = data.merge(code_bdong, how='left', left_on='지역코드', right_on='시군구코드')
            logging.info(f"[LEASE] 병합 후 컬럼: {list(merged.columns)}")
            group_col = None
            if '시도명_y' in merged.columns and '시군구명' in merged.columns:
                merged['시도_시군구'] = (merged['시도명_y'].fillna('') + '_' + merged['시군구명'].fillna('')).str.strip('_')
                group_col = '시도_시군구'
            elif '시군구명' in merged.columns:
                group_col = '시군구명'
            elif '시군구코드' in merged.columns:
                group_col = '시군구코드'
            else:
                group_col = '지역코드'

            logging.info(f"지니계수 계산 시작 - 지역 단위: {group_col}")
            gini_calculator = calculate_gini.GiniCalculator(merged)
            gini_result = gini_calculator.calculate_stats(group_col)
            base = merged
        elif region_unit == '법정동':
            # 법정동에 시군구명을 붙여서 구분하기 위해 지역코드로 시군구명 매핑
            data['지역코드'] = data['지역코드'].astype(str).str.zfill(5)
            code_bdong = pdr.code_bdong()[["시군구코드", "시군구명", "시도명"]].drop_duplicates("시군구코드")
            code_bdong.columns = code_bdong.columns.str.strip()
            code_bdong['시군구코드'] = code_bdong['시군구코드'].astype(str).str.zfill(5)
            merged = data.merge(code_bdong, how='left', left_on='지역코드', right_on='시군구코드')
            
            # 법정동명 정리 및 시군구명_법정동 형태로 결합
            merged['법정동'] = merged['법정동'].astype(str).str.strip()
            merged['시군구명_법정동'] = (merged['시군구명'].fillna('') + '_' + merged['법정동'].fillna('')).str.strip('_')
            
            logging.info(f"[LEASE] 법정동 매핑 후 컬럼: {list(merged.columns)}")
            logging.info(f"[LEASE] 시군구명_법정동 샘플: {merged['시군구명_법정동'].head().tolist()}")
            
            gini_calculator = calculate_gini.GiniCalculator(merged)
            gini_result = gini_calculator.calculate_stats('시군구명_법정동')
            base = merged
        else:
            logging.error(f"[LEASE] 지원하지 않는 지역단위: {region_unit}")
            return None

        if gini_result is None:
            logging.error("[LEASE] 지니계수 결과가 None")
            return None

        return {
            'raw_data': base,
            'bdong_gini': gini_result['grouped'],
        }
    except Exception as e:
        logging.error(f"[LEASE] process_election_data 오류: {str(e)}")
        raise

def create_folder():
    import datetime
    
    # 시간에 따라 폴더 생성
    date_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    directory = f'data/processed/지니계수_변환과정/{date_time}'
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"결과 저장 디렉토리 생성: {directory}")
    return directory

def save_results(results, election_name, region_unit, directory, start_date=None, end_date=None):
    """
    전월세 전용 저장: 원본/지니계수만 저장
    """
    import datetime

    # 시작/끝 날짜 문자열 준비
    def fmt(d):
        if not d:
            return '00000000'
        return d if isinstance(d, str) else d.strftime('%Y%m%d')

    start_date_formatted = fmt(start_date)
    end_date_formatted = fmt(end_date)

    file_path = os.path.join(directory, f'{start_date_formatted}_{end_date_formatted}_{election_name}_{region_unit}_지니계수.xlsx')
    try:
        logging.info(f"{election_name} 전월세 결과 저장 시작")
        with pd.ExcelWriter(file_path) as writer:
            result = results[election_name]
            result['raw_data'].head(5000).to_excel(writer, sheet_name='전월세_원본', index=False)
            sheet_name = f"{region_unit}_별_지니계수"
            result['bdong_gini'].head(5000).to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info(f"전월세 결과 저장 완료: {file_path}")
    except Exception as e:
        logging.error(f"전월세 결과 저장 중 오류 발생: {str(e)}")
        raise

def process_and_save_all_elections(election_list, db_path, table_name, start_date=None, end_date=None, region_unit='시군구'):
    """
    모든 선거 데이터를 처리하고 저장하는 함수
    """
    try:
        logging.info(f"데이터 처리 시작 - 선거: {list(election_list.keys())}, 기간: {start_date} ~ {end_date}")
        election_data = load_data.load_election_data(election_list, db_path, table_name, start_date, end_date)
        results = {}
        folder = create_folder()
        
        for election_name, election_date in election_list.items():
            logging.info(f"{election_name} 처리 시작...")
            df = election_data[election_name]
            result = process_election_data(df, region_unit)
            
            if result is None:
                logging.error(f"{election_name} 처리 실패")
                continue
                
            results[election_name] = {
                'raw_data': result['raw_data'],
                'bdong_gini': result['bdong_gini']
            }
            
            logging.info(f"{election_name} 처리 완료")
            logging.info(f"{election_name} 저장 시작...")
            save_results(results, election_name, region_unit, folder, start_date, end_date)
            
        logging.info("모든 선거 데이터 처리 및 저장 완료")
        return results
        
    except Exception as e:
        logging.error(f"데이터 처리 중 오류 발생: {str(e)}")
        raise