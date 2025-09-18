# 🐍 Python 3.9 버전에서 PublicDataReader 네트워크 오류 해결방법

## 🔍 문제 진단
Python 3.9에서 `raw.githubusercontent.com` 접근 시 발생하는 문제:
- SSL 인증서 검증 오류
- urllib3/requests 버전 호환성 문제
- 네트워크 보안 설정 차이

## 🛠️ 해결방법 (우선순위별)

### 방법 1: 라이브러리 버전 호환성 수정
```bash
# 호환되는 버전으로 설치
pip install urllib3==1.26.18
pip install requests==2.28.2
pip install certifi --upgrade
```

### 방법 2: SSL 인증서 문제 해결
```python
# matching.py 상단에 추가
import ssl
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 또는 환경변수 설정
import os
os.environ['PYTHONHTTPSVERIFY'] = '0'
```

### 방법 3: 로컬 데이터 사용 (가장 안전한 방법)
```python
# 1단계: 데이터 미리 다운로드 (네트워크 연결이 가능할 때)
import PublicDataReader as pdr
import pandas as pd

try:
    # 데이터 다운로드 및 저장
    conn_code = pdr.code_hdong_bdong()
    hdong_code = pdr.code_hdong()
    bdong_code = pdr.code_bdong()
    
    # 로컬 저장
    conn_code.to_excel('data/raw/법정동_행정동_연결코드.xlsx', index=False)
    hdong_code.to_excel('data/raw/행정동코드.xlsx', index=False)
    bdong_code.to_excel('data/raw/법정동코드.xlsx', index=False)
    
    print("데이터 저장 완료!")
except Exception as e:
    print(f"다운로드 실패: {e}")

# 2단계: matching.py 수정
class Matcher:
    def __init__(self, data):
        self.data = data
        
        try:
            # 온라인에서 시도
            self.conn_code = pdr.code_hdong_bdong()
            self.code_hdong = pdr.code_hdong()
            self.code_bdong = pdr.code_bdong()
        except:
            # 실패시 로컬 파일 사용
            print("네트워크 오류로 로컬 파일 사용")
            self.conn_code = pd.read_excel('data/raw/법정동_행정동_연결코드.xlsx')
            self.code_hdong = pd.read_excel('data/raw/행정동코드.xlsx')
            self.code_bdong = pd.read_excel('data/raw/법정동코드.xlsx')
```

### 방법 4: PublicDataReader 버전 다운그레이드
```bash
# 안정적인 구버전 사용
pip install PublicDataReader==1.0.3
```

## 🎯 권장사항
1. **방법 3 (로컬 데이터 사용)**을 가장 추천
2. 네트워크 의존성을 완전히 제거
3. 안정적인 실행 보장

## 📋 단계별 실행 가이드
1. 네트워크 연결이 가능한 환경에서 데이터 다운로드
2. 로컬 파일로 저장
3. 코드 수정하여 로컬 파일 우선 사용
4. 교수님께 수정된 코드 전달
