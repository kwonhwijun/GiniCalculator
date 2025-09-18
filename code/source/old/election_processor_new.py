import pandas as pd
from source import calculate_gini, load_data, preprocess, matching

def process_election_data(election_data, election_name, election_date):
    """선거 데이터 처리 및 지니계수 계산 함수"""
    result = {}
    
    # 1. 데이터 전처리
    processor = preprocess.DataProcessor(election_data[election_name])
    processed_data = processor.preprocessing()
    
    # 2. 부동산 거래에 행정동 코드 매칭
    matcher = matching.Matcher(processed_data)

    code_선거일_법정동 = matcher.gen_bdong(election_date)
    code_현재_법정동 = matcher.gen_bdong('240801')  # 특정 기준일을 상수화
    code_법정동_매핑 = matcher.valid_bdong(election_date)
    code_법정동_매핑_masked = matcher.mask_bdong(code_법정동_매핑, code_선거일_법정동)
    
    # 3. 법정동 변환 코드 로드 및 매핑
    거래데이터 = map_legal_codes(processed_data, election_name)
    
    # 4. 법정동 매핑 처리 후 필터링
    election_date = pd.to_datetime(election_date, format='%y%m%d')
    code_법정동_행정동 = filter_legal_codes(matcher.conn_code, election_date)

    # 5. 행정동코드에 선거구 매핑
    거래_선거구, na_count, row_count = match_election_districts(거래데이터, election_name)
    
    # 6. 선거구별 지니계수 계산
    bdong_jini, final_row_count = calculate_gini_for_districts(거래_선거구, election_data, election_name)
    
    return 거래데이터, code_선거일_법정동, code_현재_법정동, code_법정동_매핑_masked, code_법정동_행정동, 거래_선거구, bdong_jini


def map_legal_codes(data, election_name):
    """법정동 변환 코드를 사용해 거래 데이터에 매핑"""
    raw_data = data.copy()
    original_row_count = raw_data.shape[0]
    
    # 변환 코드 로드 및 매핑
    try:
        변환코드 = pd.read_excel(f'data/processed/법정동_변환코드/{election_name}_법정동_변환코드.xlsx')
        변환코드['법정동코드'] = 변환코드['법정동코드'].astype('string')
        변환코드['과거시점_법정동코드'] = 변환코드['과거시점_법정동코드'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None

    mapping_dict = 변환코드.set_index('법정동코드')['과거시점_법정동코드'].to_dict()
    
    # 매핑 처리
    raw_data['법정동코드'] = raw_data['법정동코드'].map(mapping_dict)
    print(f"Original Row Count: {original_row_count}, Mapped Rows: {raw_data['법정동코드'].notnull().sum()}")
    
    return raw_data


def filter_legal_codes(df, election_date):
    """법정동 변환 필터링"""
    filtered_code = df[(df['생성일자'] < election_date) & (df['말소일자'] > election_date)]
    return filtered_code[filtered_code["읍면동명"] != ""]


def match_election_districts(거래데이터, election_name):
    """거래 데이터에 선거구 매핑"""
    try:
        행정동_선거구 = pd.read_excel(f'data/processed/선거구수기2/{election_name}_선거구_행정동_매칭_수기2.xlsx')
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None, None, None

    행정동_선거구['행정동코드'] = 행정동_선거구['행정동코드'].astype('string')
    matched_data = pd.merge(거래데이터, 행정동_선거구, how='left', on='행정동코드')
    
    na_count = matched_data['district'].isna().sum()
    row_count = matched_data.shape[0]
    print(f"NA Count: {na_count}, Row Count: {row_count} ({na_count/row_count:.2%})")

    return matched_data, na_count, row_count


def calculate_gini_for_districts(거래_선거구, election_data, election_name):
    """선거구별 지니계수 계산"""
    GiniCalculator = calculate_gini.GiniCalculator(거래_선거구)
    gini_result = GiniCalculator.calculate_stats('district')
    bdong_jini = gini_result['grouped']
    
    final_row_count = gini_result['row_count']
    mapping_rate = final_row_count / election_data[election_name].shape[0]
    print(f"Final Row Count: {final_row_count}, Mapping Rate: {mapping_rate:.2%}")
    
    return bdong_jini, final_row_count


def create_folder():
    """결과를 저장할 폴더 생성"""
    import datetime
    import os
    date_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%')
    directory = f'data/processed/선거구_변환_과정/{date_time}'
    os.makedirs(directory, exist_ok=True)
    return directory


def save_results(results, election_name, directory):
    """처리된 결과를 Excel 파일로 저장하는 함수"""
    with pd.ExcelWriter(f'{directory}/{election_name}_매핑과정.xlsx') as writer:
        result = results[election_name]
        result['거래_원본'].head(5000).to_excel(writer, sheet_name='아파트_원본', index=False)
        result['code_선거일_법정동'].to_excel(writer, sheet_name='선거일_법정동코드', index=False)
        result['code_현재_법정동'].to_excel(writer, sheet_name='현행_법정동코드', index=False)
        result['code_법정동_매핑'].to_excel(writer, sheet_name='법정동_매핑', index=False)
        result['code_법정동_행정동'].to_excel(writer, sheet_name='법정동_행정동_매핑코드', index=False)
        result['거래_선거구'].head(5000).to_excel(writer, sheet_name='아파트_선거구', index=False)
        result['선거구별_지니계수'].to_excel(writer, sheet_name='선거구별_지니계수', index=False)
    print(f"Completed Saving {election_name} at {directory}/{election_name}_매핑과정.xlsx")


def process_and_save_all_elections(선거리스트, db_path):
    """모든 선거 데이터를 처리하고 저장하는 함수"""
    election_data = load_data.load_election_data(선거리스트, db_path)
    results = {}
    folder = create_folder()
    
    for election_name, election_date in 선거리스트.items():
        print(f"Processing {election_name}...")
        try:
            result = process_election_data(election_data, election_name, election_date)
            results[election_name] = result
            save_results(results, election_name, folder)
        except Exception as e:
            print(f"Error processing {election_name}: {e}")
    
    return results