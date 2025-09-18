import pandas as pd
import logging
from source import calculate_gini, load_data, preprocess, matching
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_election_data(election_data, election_name, election_date, region_unit, cur_date='240801'):
    """
    선거 데이터 처리 및 지니계수 계산 함수

    Parameters:
        election_data (dict): 선거 데이터 딕셔너리
        election_name (str): 선거 이름
        election_date (str): 선거 날짜 (YYMMDD 형식)
        region_unit (str): 지역 단위 ('시군구', '읍면동', '선거구')
        cur_date (str): 수집시점 날짜 (기본값 '240801')

    Returns:
        dict: 처리된 결과들을 포함하는 딕셔너리
    """
    try:
        logging.info(f"선거 데이터 처리 시작 - 선거: {election_name}, 날짜: {election_date}, 지역단위: {region_unit}")
        
        if election_name not in election_data:
            logging.error(f"선거 데이터를 찾을 수 없음: {election_name}")
            return None
            
        logging.info("0. RAW 데이터의 수: %s", election_data[election_name].shape)
        processor = preprocess.DataProcessor(election_data[election_name])
        processed_data = processor.preprocessing()
        logging.info("1. 전처리된 데이터의 수: %s", processed_data.shape)
        
        # 법정동코드 변환
        logging.info("법정동코드 변환 시작")
        matcher = matching.Matcher(processed_data)
        code_election_day = matcher.gen_bdong(election_date)  # 선거일 법정동코드
        code_current = matcher.gen_bdong(cur_date)  # 현재 법정동코드
        
        raw_data = processed_data.copy()
        mapping_file = f"data/processed/법정동_변환코드/{election_name}_법정동_변환코드.xlsx"
        
        logging.info(f"법정동 변환코드 파일 로드: {mapping_file}")
        if not os.path.exists(mapping_file):
            logging.error(f"법정동 변환코드 파일을 찾을 수 없음: {mapping_file}")
            return None
            
        mapping_df = pd.read_excel(mapping_file)
        
        # 데이터 타입 통일 및 변환
        logging.info("데이터 타입 변환 및 매핑 시작")
        raw_data['법정동코드'] = raw_data['법정동코드'].astype('string')
        mapping_df['법정동코드'] = mapping_df['법정동코드'].astype('string')
        mapping_df['과거시점_법정동코드'] = mapping_df['과거시점_법정동코드'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
        mapping_dict = mapping_df.set_index('법정동코드')['과거시점_법정동코드'].to_dict()
        
        data_mapped = raw_data.copy()
        data_mapped['현재시점_법정동코드'] = data_mapped['법정동코드']
        data_mapped['법정동코드'] = data_mapped['법정동코드'].map(mapping_dict)
        data_mapped['법정동코드'] = data_mapped['법정동코드'].astype('string')
        logging.info("2. 법정동 코드 복원 데이터의 수: %s, 매칭 안된 행: %d", 
                     data_mapped.shape, data_mapped['법정동코드'].isna().sum())
        
        # 행정동 코드 매칭
        logging.info("행정동 코드 매칭 시작")
        election_date_dt = pd.to_datetime(election_date, format='%y%m%d')
        code_df = matcher.conn_code
        filtered_code = code_df[(code_df['생성일자'] < election_date_dt) & (code_df['말소일자'] > election_date_dt)]
        filtered_code = filtered_code[filtered_code["읍면동명"] != ""]
        code_admin = filtered_code
        
        logging.info(f"행정동 코드 필터링 결과: {len(filtered_code)}개 행정동")
        merged_data = pd.merge(data_mapped,
                             filtered_code[["법정동코드", "행정동코드", "생성일자", "말소일자"]],
                             how='left',
                             on='법정동코드')
        logging.info("3. 행정동 코드 매칭 데이터의 수: %s, 매칭 안된 행: %d", 
                     data_mapped.shape, merged_data['행정동코드'].isna().sum())
        
        # 선거구 매칭
        logging.info("선거구 매칭 시작")
        district_mapping_file = f"data/processed/선거구수기2/{election_name}_선거구_행정동_매칭_수기2.xlsx"
        
        if not os.path.exists(district_mapping_file):
            logging.error(f"선거구 매핑 파일을 찾을 수 없음: {district_mapping_file}")
            return None
            
        district_df = pd.read_excel(district_mapping_file)
        district_df['행정동코드'] = district_df['행정동코드'].astype('string')
        logging.info(f"선거구 매핑 파일 로드 완료: {len(district_df)}개 행정동-선거구 매핑")
        
        # 불필요한 컬럼 제거
        columns_to_exclude = ['시도명', '시군구명', '읍면동명']
        merged_data = merged_data.drop(columns=[col for col in columns_to_exclude if col in merged_data.columns])
        
        merged_district = merged_data.merge(district_df, how='left', on='행정동코드')
        logging.info("4. 선거구 매칭 데이터의 수: %s, 매칭 안된 행: %d", 
                     data_mapped.shape, merged_district['district'].isna().sum())
        
        # 선거구별 지니계수 계산
        logging.info("지니계수 계산 시작")
        merged_district['시도_시군구'] = merged_district['시도명'] + '_' + merged_district['시군구명']
        merged_district['시도_시군구_읍면동'] = merged_district['시도명'] + '_' + merged_district['시군구명'] + '_' + merged_district['읍면동명']
        # 시도명과 district를 결합한 새로운 칼럼 생성
        merged_district['시도명district'] = merged_district['시도명'] + '_' + merged_district['district']
        gini_calculator = calculate_gini.GiniCalculator(merged_district)
        
        # 디버깅: region_unit 값 확인 및 로그 출력
        # '행정동'과 '읍면동'은 동일 취지로 처리
        valid_units = ["시군구", "읍면동", "행정동", "선거구"]
        logging.info("선택된 지역 단위 (region_unit): %s", region_unit)
        logging.info("유효한 지역 단위 목록: %s", valid_units)

        if region_unit == "시군구":
            region_column = '시도_시군구'
        elif region_unit in ["읍면동", "행정동"]:
            region_column = '시도_시군구_읍면동'
        elif region_unit == '선거구':
            region_column = '시도명district'  # 수정: 결합된 칼럼 사용
            logging.info("선거구 단위로 지니계수 계산 중...")
        else:
            error_msg = f"유효하지 않은 지역 단위입니다: {region_unit}"
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        logging.info(f"지니계수 계산에 사용될 컬럼: {region_column}")
        gini_result = gini_calculator.calculate_stats(region_column)
        
        if gini_result is None:
            logging.error("지니계수 계산 결과가 None입니다")
            return None
            
        result = {
            'raw_data': raw_data,
            'code_election_day': code_election_day,
            'code_current': code_current,
            'mapping_df': mapping_df,
            'code_district': district_df,
            'merged_admin': merged_data,
            'code_admin': code_admin,
            'merged_district': merged_district,
            'bdong_gini': gini_result['grouped']
        }
        
        logging.info(f"{election_name} 데이터 처리 완료")
        return result
    
    except Exception as e:
        logging.error("Error in process_election_data: %s", str(e))
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
    처리된 결과를 Excel 파일로 저장하는 함수
    """
    import datetime
    
    # 현재 날짜와 시간
    now = datetime.datetime.now()
    save_date = now.strftime('%Y%m%d')
    save_time = now.strftime('%H%M')
    
    # 시작 날짜와 끝 날짜 형식 변환
    if start_date:
        if isinstance(start_date, str):
            start_date_formatted = start_date
        else:
            start_date_formatted = start_date.strftime('%Y%m%d')
    else:
        start_date_formatted = '00000000'
        
    if end_date:
        if isinstance(end_date, str):
            end_date_formatted = end_date
        else:
            end_date_formatted = end_date.strftime('%Y%m%d')
    else:
        end_date_formatted = '00000000'
    
    file_path = os.path.join(directory, f'{start_date_formatted}_{end_date_formatted}_{election_name}_{region_unit}_지니계수.xlsx')
    try:
        logging.info(f"{election_name} 결과 저장 시작")
        with pd.ExcelWriter(file_path) as writer:
            result = results[election_name]
            result['raw_data'].head(5000).to_excel(writer, sheet_name='아파트_원본', index=False)
            result['code_election_day'].head(5000).to_excel(writer, sheet_name='선거일_법정동코드', index=False)
            result['code_current'].head(5000).to_excel(writer, sheet_name='현행_법정동코드', index=False)
            result['mapping_df'].head(5000).to_excel(writer, sheet_name='법정동_매핑', index=False)
            result['merged_admin'].head(5000).to_excel(writer, sheet_name='법정동_행정동_매핑코드', index=False)
            result['merged_district'].head(5000).to_excel(writer, sheet_name='아파트_행정동', index=False)
            result['code_district'].to_excel(writer, sheet_name='행정동_선거구_매핑코드', index=False)
            result['merged_district'].head(5000).to_excel(writer, sheet_name='아파트_선거구', index=False)
            result['bdong_gini'].head(5000).to_excel(writer, sheet_name='선거구별_지니계수', index=False)
            
            # 누락 데이터 저장
            누락_선거구 = list(set(result['code_district'].district) - set(result['merged_district'].district))
            pd.DataFrame(누락_선거구, columns=['누락된_선거구']).to_excel(writer, sheet_name='누락_선거구')
            result['merged_admin'].head(5000).to_excel(writer, sheet_name='누락_행정동코드', index=False)
            
        logging.info(f"결과 저장 완료: {file_path}")
    except Exception as e:
        logging.error(f"결과 저장 중 오류 발생: {str(e)}")
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
            result = process_election_data(election_data, election_name, election_date, region_unit)
            
            if result is None:
                logging.error(f"{election_name} 처리 실패")
                continue
                
            results[election_name] = {
                'raw_data': result['raw_data'],
                'code_election_day': result['code_election_day'],
                'code_current': result['code_current'],
                'code_district': result['code_district'],
                'mapping_df': result['mapping_df'],
                'merged_admin': result['merged_admin'],
                'code_admin': result['code_admin'],
                'merged_district': result['merged_district'],
                'bdong_gini': result['bdong_gini']
            }
            
            # 누락 항목 계산
            results[election_name]['누락_선거구'] = set(results[election_name]['code_district'].district) - set(results[election_name]['merged_district'].district)
            results[election_name]['누락_행정동코드'] = results[election_name]['code_district'][results[election_name]['code_district'].district.isin(results[election_name]['누락_선거구'])]
            
            logging.info(f"{election_name} 처리 완료")
            logging.info(f"{election_name} 저장 시작...")
            save_results(results, election_name, region_unit, folder, start_date, end_date)
            
        logging.info("모든 선거 데이터 처리 및 저장 완료")
        return results
        
    except Exception as e:
        logging.error(f"데이터 처리 중 오류 발생: {str(e)}")
        raise