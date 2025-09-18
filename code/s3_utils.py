"""
AWS S3 유틸리티 함수들
"""
import boto3
import os
import streamlit as st
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

def get_s3_client():
    """
    S3 클라이언트 생성
    환경변수 또는 Streamlit secrets에서 AWS 자격증명 가져오기
    """
    try:
        # 로컬 환경변수 우선 확인
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-2')
        
        if aws_access_key_id and aws_secret_access_key:
            # 환경변수 사용 (로컬 개발시)
            return boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=aws_region
            )
        
        # Streamlit Cloud의 secrets 사용 (배포시)
        elif hasattr(st, 'secrets') and 'aws' in st.secrets:
            return boto3.client(
                's3',
                aws_access_key_id=st.secrets['aws']['access_key_id'],
                aws_secret_access_key=st.secrets['aws']['secret_access_key'],
                region_name=st.secrets['aws']['region']
            )
        
        # 기본 AWS 자격증명 체인 사용
        else:
            return boto3.client('s3')
            
    except Exception as e:
        st.error(f"AWS S3 클라이언트 생성 실패: {e}")
        return None

def download_db_from_s3(bucket_name, s3_key, local_path):
    """
    S3에서 데이터베이스 파일 다운로드
    
    Args:
        bucket_name (str): S3 버킷 이름
        s3_key (str): S3 객체 키 (파일 경로)
        local_path (str): 로컬 저장 경로
    
    Returns:
        bool: 다운로드 성공 여부
    """
    s3_client = get_s3_client()
    if not s3_client:
        return False
    
    try:
        # 로컬 디렉토리 생성
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # 파일이 이미 존재하는지 확인
        if os.path.exists(local_path):
            st.info(f"데이터베이스 파일이 이미 존재합니다: {local_path}")
            return True
        
        # S3에서 파일 다운로드
        st.info("S3에서 데이터베이스 파일을 다운로드 중입니다... (최초 실행시에만)")
        
        # 프로그레스 바 표시
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(bytes_transferred):
            # 대략적인 파일 크기 (2.3GB)
            total_size = 2.3 * 1024 * 1024 * 1024  # 바이트 단위
            percentage = min(bytes_transferred / total_size, 1.0)
            progress_bar.progress(percentage)
            status_text.text(f"다운로드 중... {bytes_transferred / (1024*1024):.1f}MB")
        
        # 실제 다운로드 (콜백은 boto3에서 직접 지원하지 않으므로 단순화)
        s3_client.download_file(bucket_name, s3_key, local_path)
        
        progress_bar.progress(1.0)
        status_text.text("다운로드 완료!")
        st.success(f"데이터베이스 파일 다운로드 완료: {local_path}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            st.error(f"S3 버킷을 찾을 수 없습니다: {bucket_name}")
        elif error_code == 'NoSuchKey':
            st.error(f"S3 객체를 찾을 수 없습니다: {s3_key}")
        else:
            st.error(f"S3 다운로드 오류: {e}")
        return False
        
    except NoCredentialsError:
        st.error("AWS 자격증명을 찾을 수 없습니다. AWS 설정을 확인해주세요.")
        return False
        
    except Exception as e:
        st.error(f"예상치 못한 오류가 발생했습니다: {e}")
        return False

def check_s3_connection(bucket_name):
    """
    S3 연결 및 버킷 접근 권한 확인
    
    Args:
        bucket_name (str): 확인할 S3 버킷 이름
    
    Returns:
        bool: 연결 성공 여부
    """
    s3_client = get_s3_client()
    if not s3_client:
        return False
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        st.error(f"S3 버킷 접근 실패: {e}")
        return False
    except Exception as e:
        st.error(f"S3 연결 확인 실패: {e}")
        return False
