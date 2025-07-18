import numpy as np
import PublicDataReader as pdr
import logging

class GiniCalculator:
    def __init__(self, data):
        self.data = data
        logging.basicConfig(level=logging.INFO)

    def gini(self, array):
        array = array.flatten().astype(float)  # 1차원 배열로 만들기 + 실수형으로
        if np.amin(array) < 0:
            array -= np.amin(array)  # 음수 값은 불가
        array += 0.0000001  # 0 방지
        array = np.sort(array)  # 거래 값을 정렬
        index = np.arange(1, array.shape[0] + 1)  # 각 배열에 인덱스 부여
        n = array.shape[0]
        return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))

    def calculate_gini_per_group(self, group_cols, value_col):
        grouped = self.data.groupby(group_cols)
        gini_results = grouped[value_col].apply(lambda x: self.gini(x.values)).reset_index()
        gini_results.columns = group_cols + ['지니계수']
        return gini_results
    
    def calculate_stats(self, region_col):
        try:
            logging.info(f"지니계수 계산 시작 - 지역 단위: {region_col}")
            self.data['년도'] = self.data['거래일자'].dt.year

            if isinstance(region_col, str):
                group_cols = [region_col]
            else:
                group_cols = region_col

            # 데이터 그룹화 전 region_col 존재 여부 확인
            if region_col not in self.data.columns:
                logging.error(f"컬럼을 찾을 수 없음: {region_col}")
                return None

            grouped = self.data.groupby(group_cols).agg(
                거래수=('거래금액', 'count'),
                평균거래금액=('거래금액', 'mean'),
                지니계수=('거래금액', lambda x: self.gini(x.values)),
                평당_평균거래금액=('평당거래금액', 'mean'),
                평당_지니계수=('평당거래금액', lambda x: self.gini(x.values))
            ).reset_index()

            logging.info(f"그룹화된 데이터 형태: {grouped.shape}")
            result_num = grouped['거래수'].sum()

            # 법정동코드 별 지니계수 
            if region_col == '법정동코드':
                code_bdong = pdr.code_bdong()
                code_bdong.rename(columns={'읍면동명': '법정동명'}, inplace=True)
                grouped = code_bdong[["시도명", "시군구명", "법정동명", "법정동코드"]].merge(grouped, on='법정동코드', how='right')
                grouped.to_csv(f"data/processed/{region_col}_지니계수.csv")
            # 행정동코드 별 지니계수
            elif region_col == '행정동코드':
                code_hdong = pdr.code_hdong()
                code_hdong.rename(columns={'읍면동명': '행정동명'}, inplace=True)
                grouped = code_hdong[["시도명", "시군구명", "행정동명", "행정동코드"]].merge(grouped, on='행정동코드', how='right')
                grouped.to_csv(f"data/processed/{region_col}_지니계수.csv")
            # 선거구 별 지니계수
            elif region_col == '시도명district':
                grouped.to_csv(f"data/processed/선거구_지니계수.csv")
                logging.info("선거구별 지니계수 계산 완료")

            result = {'row_count': result_num, 'grouped': grouped}
            logging.info("지니계수 계산 완료")
            return result

        except Exception as e:
            logging.error(f"지니계수 계산 중 오류 발생: {str(e)}")
            raise

# test = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 100])
# calculator = GiniCalculator(test)
# calculator.gini(test)

