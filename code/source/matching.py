import pandas as pd
import PublicDataReader as pdr

class Matcher:
    def __init__(self, data) :
        self.data = data
        self.conn_code = pdr.code_hdong_bdong()
        self.code_hdong = pdr.code_hdong()
        self.code_bdong = pdr.code_bdong()

        self.conn_code['생성일자'] = pd.to_datetime(self.conn_code['생성일자'], format='%Y%m%d', errors='coerce')
        self.conn_code['말소일자'] = pd.to_datetime(self.conn_code['말소일자'], format='%Y%m%d', errors='coerce')
        self.conn_code['말소일자'] = self.conn_code['말소일자'].fillna(pd.Timestamp.max)

        
        self.code_hdong['생성일자'] = pd.to_datetime(self.code_hdong['생성일자'], format='%Y%m%d', errors='coerce')
        self.code_hdong['말소일자'] = pd.to_datetime(self.code_hdong['말소일자'], format='%Y%m%d', errors='coerce')
        self.code_hdong['말소일자'] = self.code_hdong['말소일자'].fillna(pd.Timestamp.max)

        self.election_df = pd.read_excel("data/raw/국회의원_지역구_읍면동_경계_13_21.xlsx")

        
        self.code_bdong['생성일자'] = pd.to_datetime(self.code_bdong['생성일자'], format = '%Y%m%d')
        self.code_bdong['말소일자'] = pd.to_datetime(self.code_bdong['말소일자'], format = '%Y%m%d')
    def bdong2hdong(self):
        matched_data = pd.merge(self.data,
                                self.conn_code[["법정동코드", "행정동코드", "생성일자", "말소일자"]],
                                how='left',
                                on='법정동코드')
        matched_data = matched_data[
            (matched_data['거래일자'] >= matched_data['생성일자']) &
            ((matched_data['거래일자'] <= matched_data['말소일자']) | (matched_data['말소일자'].isna()))
            ]
        
        # 균등하게 매칭하기 위해 법정동코드 별로 행정동코드를 무작위로 선택
        # matched_data['random'] = matched_data.groupby('법정동코드').cumcount()
        # matched_data = matched_data.sort_values('random').drop_duplicates(subset=['법정동코드', '거래금액', '일련번호', '거래일자'], keep='first')


        #def random_sample(group):
        #    return group.sample(1)
        #matched_data = matched_data.groupby('법정동코드', group_keys=False).apply(random_sample)

        self.data = matched_data
        return matched_data

    
    def hdong2elect(self) :
        election_date = {'18' : '2008-04-09', '19' : '2012-04-11', '20' : '2016-04-13', '21': '2020-04-15', '22': '2024-04-10'}
        election_df = self.election_df
        election_df.rename(columns={'sigungu' : '시군구명', 'e_emd' : '읍면동명'}, inplace = True)
        election_df["선거일"] = election_df["election"].str[1:3].map(election_date)
        election_df['선거일'] = pd.to_datetime(election_df['선거일'])

        merged_df = pd.merge(election_df, self.code_hdong, how='right', left_on=['시군구명', '읍면동명'], right_on=['시군구명', '읍면동명'], indicator=True)
    
        # Filter based on date conditions
        # merged_df = merged_df[(merged_df['선거일'] >= merged_df['생성일자']) & (merged_df['선거일'] <= merged_df['말소일자'])]

        # Drop duplicates and select relevant columns
        # merged_df = merged_df.drop_duplicates(subset=["election", '시군구명', '읍면동명', '행정동코드','선거일'])
        # result_df = merged_df[['election', '시군구명', '읍면동명', '선거일', '행정동코드', 'district']]
        return merged_df
    
    def gen_bdong(self, base_date):
        code_bdong = self.code_bdong
        # 기준일 기준 법정동코드 생성하기 
        base_date = pd.to_datetime(base_date, format = '%y%m%d')
        is_vaild_생성일자 = code_bdong['생성일자'] < base_date
        is_vaild_말소일자 = (base_date < code_bdong['말소일자']) | (code_bdong['말소일자'].isna())
        valid_bdong = code_bdong[is_vaild_말소일자 & is_vaild_생성일자]
        return valid_bdong

    def valid_bdong(self, base_date):
        code_bdong = self.code_bdong # 법정동코드 불러오기
        base_date = pd.to_datetime(base_date, format = '%y%m%d') # 기준일 시계열 데이터로 변환
        cur_bdong = self.gen_bdong('240801').copy()
        is_valid_생성일자 = cur_bdong['생성일자'] < base_date
        is_vaild_말소일자 = (base_date < code_bdong['말소일자']) | (code_bdong['말소일자'].isna())
        cur_bdong.loc[:, '선거시점_존재여부'] = is_valid_생성일자 & is_vaild_말소일자

        # 과거시점_법정동코드 열 생성
        cur_bdong.loc[:, '과거시점_법정동코드'] = cur_bdong['법정동코드'].where(cur_bdong['선거시점_존재여부'], other='')

        누락법정동 = (cur_bdong['선거시점_존재여부'] == False).sum()
        print(f"달라진 법정동의 수 : {누락법정동}")
        return cur_bdong

    def mask_bdong(self, code_법정동_매핑, code_선거일_법정동):
        # 과거시점_법정동코드가 비어있는 행 찾기
        mask = code_법정동_매핑['과거시점_법정동코드'].isna()

        # 조건에 맞는 행 찾아서 과거시점_법정동코드 채우기
        for idx, row in code_법정동_매핑[mask].iterrows():
            matching_rows = code_선거일_법정동[
                (code_선거일_법정동['동리명'] == row['동리명']) &
                (code_선거일_법정동['시군구코드'] == row['시군구코드'])
            ]
            
            if not matching_rows.empty:
                code_법정동_매핑.loc[idx, '과거시점_법정동코드'] = matching_rows.iloc[0]['법정동코드']

        return code_법정동_매핑
