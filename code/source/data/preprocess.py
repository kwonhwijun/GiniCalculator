import pandas as pd

class DataProcessor:
    def __init__(self, data):
        self.data = data

    def explore_data(self):
        print(f"데이터 타입 : {self.data.dtypes}")
        print(f"결측값 ㅣ {self.data.isnull().sum()}")
        print(f"요약 통계량 : {self.data.describe()}")


    def preprocessing(self):
        # 중복 행 제거
        self.data.drop_duplicates(inplace = True)

        # 결측값이 많은 칼럼 제거
        threshold = len(self.data) * 0.5
        self.data.dropna(thresh = threshold, axis = 1, inplace = True)

        # 필요 없는 칼럼 제거
        drop_cols = ['건축년도', '층', '도로명건물본번호코드', '도로명건물부번호코드', '도로명일련번호코드', '도로명지상지하코드'
                     ,'법정동본번코드', '법정동부번코드']
        for col in drop_cols :
            if col in self.data.columns : self.data.drop(columns = [col], inplace = True)

        # 거래일자 칼럼 생성
        self.data["거래일자"] = pd.to_datetime(self.data[['년', '월', '일']].astype(str).agg('-'.join, axis = 1))
        self.data.drop(columns = ['년', '월', '일'], inplace = True)
        
        # 데이터 타입 변경
        if '거래금액' in self.data.columns :  # 거래금액을 정수형으로
            self.data["거래금액"] = self.data["거래금액"].str.replace(",", "").astype(int)
            
        if '전용면적' in self.data.columns : # 전용면적을 실수형으로
            self.data["전용면적"] = self.data["전용면적"].astype(float) # 법정동코드 합치기
            
        if '전용면적' in self.data.columns and '거래금액' in self.data.columns :
            self.data["평당거래금액"] = self.data["거래금액"]/self.data["전용면적"] * 3.3058 # 1평은 3.3058m^2

        if '법정동시군구코드' in self.data.columns and '법정동읍면동코드' in self.data.columns:
            self.data["법정동코드"] = self.data["법정동시군구코드"] + self.data["법정동읍면동코드"]

        return self.data