import pandas as pd
import numpy as np
import PublicDataReader as pdr

class DataProcessor:
    def __init__(self, data):
        self.data = data

    def explore_data(self):
        print(f"데이터 타입 : {self.data.dtypes}")
        print(f"결측값 ㅣ {self.data.isnull().sum()}")
        print(f"요약 통계량 : {self.data.describe()}")


    def preprocessing(self):
        # Create a copy of the original DataFrame
        data_copy = self.data.copy()

        # 중복 행 제거
        data_copy.drop_duplicates(inplace = True)

        # 결측값이 많은 칼럼 제거
        threshold = len(data_copy) * 0.5
        data_copy.dropna(thresh = threshold, axis = 1, inplace = True)

        # 필요 없는 칼럼 제거
        drop_cols = ['건축년도', '층', '도로명건물본번호코드', '도로명건물부번호코드', '도로명일련번호코드', '도로명지상지하코드'
                    ,'법정동본번코드', '법정동부번코드']
        for col in drop_cols :
            if col in data_copy.columns : data_copy.drop(columns = [col], inplace = True)

        # 거래일자 칼럼 생성 (강건성 보강)
        for col in ['년', '월', '일']:
            if col in data_copy.columns:
                data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce')
        # 일 결측은 1일로 보정
        if '일' in data_copy.columns:
            data_copy['일'] = data_copy['일'].fillna(1)
        try:
            data_copy["거래일자"] = pd.to_datetime(
                data_copy[['년', '월', '일']].astype(int).astype(str).agg('-'.join, axis=1),
                errors='coerce'
            )
        except Exception:
            # fallback: 매핑 방식
            if set(['년','월','일']).issubset(data_copy.columns):
                data_copy["거래일자"] = pd.to_datetime(
                    {
                        'year': data_copy['년'].astype('Int64').fillna(2000),
                        'month': data_copy['월'].astype('Int64').fillna(1),
                        'day': data_copy['일'].astype('Int64').fillna(1)
                    }, errors='coerce'
                )
            else:
                # 최후 수단: 전부 NaT
                data_copy["거래일자"] = pd.NaT

        # 전월세 데이터(보증금액/월세금액 존재)의 표준화 처리
        if {'보증금액', '월세금액'}.issubset(set(data_copy.columns)):
            data_copy = self._preprocess_lease(data_copy)
        else:
            # 매매 데이터 처리
            data_copy = self._preprocess_sale(data_copy)
        
        return data_copy

    def _preprocess_sale(self, df: pd.DataFrame) -> pd.DataFrame:
        # 데이터 타입 변경 (매매)
        if '거래금액' in df.columns and df["거래금액"].dtype == 'object':
            if df["거래금액"].str.contains(",").any():
                df["거래금액"] = df["거래금액"].str.replace(",", "", regex=False).astype(int)

        if '전용면적' in df.columns:
            df["전용면적"] = df["전용면적"].astype(float)

        if '전용면적' in df.columns and '거래금액' in df.columns:
            df["평당거래금액"] = df["거래금액"] / df["전용면적"] * 3.3058

        if '법정동시군구코드' in df.columns and '법정동읍면동코드' in df.columns:
            df["법정동코드"] = df["법정동시군구코드"] + df["법정동읍면동코드"]
        return df

    def _preprocess_lease(self, df: pd.DataFrame) -> pd.DataFrame:
        # 금액 컬럼 클린업
        for col in ["보증금액", "월세금액"]:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(",", "", regex=False).replace("", "0").astype(int)
            else:
                df[col] = df[col].fillna(0).astype(int)

        # 시군구코드 → 시도명 매핑 준비 (타입/포맷 정규화)
        code_bdong = pdr.code_bdong()[["시도명", "시군구코드", "동리명", "법정동코드"]].copy()
        code_bdong['시군구코드'] = code_bdong['시군구코드'].astype(str).str.zfill(5)
        code_bdong['동리명'] = code_bdong['동리명'].astype(str).str.strip()
        code_bdong.drop_duplicates(subset=["시군구코드", "동리명"], inplace=True)

        # 거래 데이터에 시도명/법정동코드 매핑 (지역코드+법정동명)
        # 좌측 키 정규화
        df['지역코드'] = df['지역코드'].astype(str).str.zfill(5)
        df['법정동'] = df['법정동'].astype(str).str.strip()

        df = df.merge(
            code_bdong,
            how='left',
            left_on=["지역코드", "법정동"],
            right_on=["시군구코드", "동리명"]
        )
        # 매칭 실패 시 대비: 시군구코드 기준 시도명만 확보
        if '시도명' not in df.columns or df['시도명'].isna().any():
            sido_lu = code_bdong.drop_duplicates(subset=['시군구코드'])[['시군구코드', '시도명']]
            df = df.merge(sido_lu, how='left', on='시군구코드', suffixes=(None, '_sido'))
            if '시도명_sido' in df.columns:
                df['시도명'] = df['시도명'].fillna(df['시도명_sido'])
                df.drop(columns=['시도명_sido'], inplace=True)

        # 전환율 로드 및 롱 포맷 변환
        rate_df = pd.read_csv('data/mapping/지역별_전월셰_전환율_2011_2025.csv')
        rate_df = rate_df[rate_df['주택유형별(1)'] == '아파트'].copy()
        rate_long = rate_df.melt(
            id_vars=['주택유형별(1)', '지역별(1)'],
            var_name='연월', value_name='전환율'
        )
        # 전환율 숫자화 및 지역/연월 키 생성
        rate_long['전환율'] = pd.to_numeric(rate_long['전환율'], errors='coerce')
        # 지역명 정규화 (양쪽 공백 제거)
        rate_long['지역'] = rate_long['지역별(1)'].astype(str).str.strip()
        rate_long['연월'] = rate_long['연월'].astype(str)
        rate_long = rate_long[['지역', '연월', '전환율']]

        # 보간: 같은 지역 내에서 시간 순으로 결측 채우기
        rate_long['연'] = rate_long['연월'].str.slice(0, 4).astype(int, errors='ignore')
        rate_long['월'] = rate_long['연월'].str.slice(5, 7)
        rate_long['월'] = pd.to_numeric(rate_long['월'], errors='coerce')
        rate_long.sort_values(['지역', '연', '월'], inplace=True)
        rate_long['전환율'] = rate_long.groupby('지역')['전환율'].transform(lambda s: s.ffill().bfill())

        # 전국 백업 키 준비
        nat_map = rate_long[rate_long['지역'] == '전국'].set_index(['연월'])['전환율']

        # 거래별 연월 키
        df['연'] = df['거래일자'].dt.year.astype(int)
        df['월_2'] = df['거래일자'].dt.month.astype(int)
        df['연월'] = df['연'].astype(str) + '.' + df['월_2'].astype(str).str.zfill(2)
        allowed_provinces = ['강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']
        df['지역키'] = df['시도명'].astype(str).str.strip()
        df['지역키'] = df['지역키'].where(df['지역키'].isin(allowed_provinces), '전국')

        # 전환율 매핑
        rate_pivot = rate_long.set_index(['지역', '연월'])['전환율']
        df['전환율'] = df.apply(
            lambda r: rate_pivot.get((r['지역키'], r['연월']), np.nan), axis=1
        )
        # 결측은 전국치로 대체
        df['전환율'] = df.apply(
            lambda r: nat_map.get(r['연월'], np.nan) if pd.isna(r['전환율']) else r['전환율'], axis=1
        )

        # 전세환산 보증금 계산: 보증금 + (월세*12) * 100 / 전환율
        df['거래금액'] = df['보증금액'] + (df['월세금액'] * 12 * 100 / df['전환율']).round().astype(int)

        # 면적 및 평당가
        if '전용면적' in df.columns:
            df['전용면적'] = df['전용면적'].astype(float)
            df['평당거래금액'] = df['거래금액'] / df['전용면적'] * 3.3058

        # 법정동코드 컬럼 정리 (가능한 경우만)
        if '법정동코드' not in df.columns and {'시군구코드', '동리명'}.issubset(df.columns):
            # 이미 merge에서 가져온 경우 존재
            pass

        return df

        
