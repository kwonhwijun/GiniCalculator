import PublicDataReader as pdr
import pandas as pd

def hdong_gen(ymd):
    hdong = pdr.code_hdong()
    hdong = hdong[~(hdong["읍면동명"] == '')] # 읍면동명이 비어있으면 제거 
    hdong['말소일자'] = pd.to_datetime(hdong['말소일자'], format = '%Y%m%d')
    hdong['생성일자'] = pd.to_datetime(hdong['생성일자'], format = '%Y%m%d')
    ymd = pd.to_datetime(ymd, format = '%y%m%d')
    # 생성일자와 말소일자 기준으로 제거
    cur_hdong_df = hdong[(hdong['생성일자']<ymd) & ((hdong['말소일자']>ymd) | hdong['말소일자'].isna())]
    return cur_hdong_df