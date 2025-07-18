import numpy as np
import PublicDataReader as pdr
class GiniCalculator :
    def __init__(self, data):
        self.data = data

    def gini(self, array):
        array = array.flatten().astype(float)  # 1차원 배열로 만들기 + 실수형으로
        if np.amin(array) < 0:
            array -= np.amin(array)  # 음수 값은 불가
        array += 0.0000001  # 0 방지
        array = np.sort(array)  # 거래 값을 정렬
        index = np.arange(1, array.shape[0] + 1)  # 각 배열에 인덱스 부여
        n = array.shape[0]
        return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))


    def calculate_gini_per_group(self, group_cols, value_col) :
        grouped = self.data.groupby(group_cols)
        gini_results = grouped[value_col].apply(lambda x: self.gini(x.values)).reset_index()
        gini_results.columns = group_cols +['지니계수']
        return gini_results
    
    def calculate_stats(self, region_col):
        self.data['년도'] = self.data['거래일자'].dt.year
        group_cols = [region_col, '년도']

        grouped = self.data.groupby(group_cols).agg(
            거래수=('거래금액', 'count'),
            평균거래금액=('거래금액', 'mean'),
            지니계수=('거래금액', lambda x: self.gini(x.values)),
            # 평당 계산
            평당_평균거래금액 = ('평당거래금액', 'mean'),
            평당_지니계수 = ('평당거래금액', lambda x : self.gini(x.values))
        ).reset_index()
        # 1. 법정동코드 별 지니계수 
        if region_col == '법정동코드':
            # 법정동 데이터 불러오기
            code_bdong = pdr.code_bdong()
            code_bdong.rename(columns = {'읍면동명' : '법정동명'}, inplace = True)
            grouped = code_bdong[["시도명", "시군구명", "법정동명", "법정동코드"]].merge(grouped, on='법정동코드', how='right')
            grouped.to_csv(f"/Users/hj/Dropbox/project/지니계수/data/processed/{region_col}_지니계수.csv")
        # 2. 행정동코드 별 지니계수
        elif region_col == '행정동코드':
            code_hdong = pdr.code_hdong()
            code_hdong.rename(columns = {'읍면동명' : '행정동명'}, inplace = True)
            grouped = code_hdong[["시도명", "시군구명", "행정동명", "행정동코드"]].merge(grouped, on='행정동코드', how='right')
            grouped.to_csv(f"/Users/hj/Dropbox/project/지니계수/data/processed{region_col}_지니계수.csv")

        return grouped

        # 거래수 계산

        # 평균 거래금액 계산

        # 지니계수 계산
# test = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 100])
# calculator = GiniCalculator(test)
# calculator.gini(test)

