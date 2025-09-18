import streamlit as st
import pandas as pd
import yaml
import pathlib
from datetime import date 
from source import election_processor, election_processor_lease
import io

# DB 경로 동적으로 생성
BASE_DIR = pathlib.Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "raw" / "RealEstate.db"

# 설정 파일 로드
config_path = BASE_DIR / 'config.yaml' # 설정 파일 경로도 동적으로 변경
with open(config_path, 'r', encoding="utf-8") as file:
    config = yaml.safe_load(file)

선거리스트 = config['elections']

election_dates = {
    '18대_국회의원': '080409',
    '19대_국회의원': '120411',
    '20대_국회의원': '160413',
    '21대_국회의원': '200415',
    '22대_국회의원': '240417'
}

# Streamlit 애플리케이션 설정
st.title("지니계수 계산기")
st.write("아파트 거래 데이터를 기준으로 지니계수를 계산합니다.")

# 앱 설명 추가
st.markdown("""
### 📌 앱 설명
1. **목적**: 아파트 거래 데이터를 기반으로 지니계수를 계산하여 지역 간 불평등 정도를 파악합니다.
2. **사용법**:
   - 날짜 범위를 선택하고, 분석할 거래 종류와 지역 단위를 지정합니다.
   - '지니계수 계산' 버튼을 클릭하여 결과를 확인하고 데이터를 다운로드할 수 있습니다.
3. **특징**: 매매 또는 전월세 데이터를 선택하여 분석 가능합니다.
""")

st.markdown("---") 

# 국회의원 선거일 정보 표시
st.header("📅 국회의원 선거일 정보")

# 데이터를 DataFrame으로 변환
election_dates_df = pd.DataFrame(list(election_dates.items()), columns=['선거명', '선거일'])

# HTML 테이블 생성 및 표시
table_html = election_dates_df.to_html(
    index=False,
    escape=False,
    border=0,
    classes='table table-bordered table-striped'
)

# CSS 스타일 추가
st.markdown("""
<style>
    .table {
        width: 100%;
        margin: 1em 0;
        border-collapse: collapse;
    }
    .table th, .table td {
        padding: 0.5em;
        text-align: left;
        border: 1px solid #ddd;
    }
    .table th {
        background-color: #f4f4f4;
    }
    .table-striped tr:nth-child(even) {
        background-color: #f9f9f9;
    }
</style>
""", unsafe_allow_html=True)

# 테이블 렌더링
st.markdown(table_html, unsafe_allow_html=True)

st.markdown("---")  # 구분선 추가

# 사용자 입력
start_date = st.date_input("시작 날짜를 선택하세요", value=date(2015, 8, 1), min_value = date(2006, 1, 1), key="start_date")
end_date = st.date_input("끝 날짜를 선택하세요", value=date(2015, 8, 31), min_value = date(2006, 1, 1), key="end_date")
선거명 = st.selectbox("선거명 선택", list(선거리스트.keys()))

# 거래 종류 선택 추가
거래_종류 = st.selectbox("거래 종류를 선택하세요", ["매매", "전월세"])

# '지역 단위' 선택 옵션 추가
지역_단위 = st.selectbox("지역 단위를 선택하세요", ["시군구", "행정동", "선거구"])

# 버튼을 클릭했을 때 처리
if st.button("지니계수 계산"):
    try:
        # 날짜를 datetime 형식으로 받아오는 부분에서 직접 strftime을 적용하기 전에 date 타입을 확인
        if isinstance(start_date, str):
            start_date_str = start_date  # 이미 str이라면 그대로 사용
        else:
            start_date_str = start_date.strftime("%y%m%d")  # datetime 객체라면 변환

        if isinstance(end_date, str):
            end_date_str = end_date  # 이미 str이라면 그대로 사용
        else:
            end_date_str = end_date.strftime("%y%m%d")  # datetime 객체라면 변환
        
        # 거래 종류에 따라 데이터 소스 설정
        if 거래_종류 == "매매":
            data_source = 'apt_raw'
            results = election_processor.process_and_save_all_elections(
                {선거명: start_date.strftime("%y%m%d")},
                DB_PATH, # 동적 경로 수정
                data_source,
                start_date=start_date_str,
                end_date=end_date_str,
                region_unit=지역_단위
                )
        
            st.success("지니계수 계산 완료!")
            
            # 결과 출력 (지니계수 데이터 요약)
            if results and 선거명 in results:
                지니계수_df = results[선거명]['bdong_gini']
                st.write("지니계수 계산 결과")
                st.dataframe(지니계수_df)  # 데이터프레임 출력

                # 선택한 지역 단위 정보 출력
                st.write(f"선택한 지역 단위: {지역_단위}")
            else:
                st.warning("계산된 지니계수 결과가 없습니다.")
            
        else:
            data_source = 'apt_lease_raw'
            results = election_processor_lease.process_and_save_all_elections(
                {선거명: start_date.strftime("%y%m%d")},
                DB_PATH, # 동적 경로 수정
                data_source,
                start_date=start_date_str,
                end_date=end_date_str,
                region_unit=지역_단위
                )
        
            st.success("지니계수 계산 완료!")
            
            # 결과 출력 (지니계수 데이터 요약)
            if results and 선거명 in results:
                지니계수_df = results[선거명]['bdong_gini']
                st.write("지니계수 계산 결과")
                st.dataframe(지니계수_df)  # 데이터프레임 출력

                # 선택한 지역 단위 정보 출력
                st.write(f"선택한 지역 단위: {지역_단위}")
            else:
                st.warning("계산된 지니계수 결과가 없습니다.")

        
        # 선택된 선거 데이터 처리
        


       
        
    except FileNotFoundError as e:
        st.error(f"파일을 찾을 수 없습니다: {e}")
    except Exception as e:
        st.error(f"오류가 발생했습니다: {e}")
