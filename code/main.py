import os
import sys
import yaml

# source 폴더에 있는 election_processor.py 파일을 import
base_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(base_dir, 'source')
sys.path.append(src_dir)
from source import election_processor

# 설정 파일 로드
config_path = 'config.yaml'
with open(config_path, 'r', encoding="utf-8") as file:
    config = yaml.safe_load(file)

db_path = config['db_path']
선거리스트 = config['elections']

if __name__ == "__main__":
    try:
        print("DB_path: ", db_path)
        target_list = ['선거구'] #['선거구', '시군구', '읍면동']
        for col in target_list :
            results = election_processor.process_and_save_all_elections(선거리스트, db_path, 'apt_raw', region_unit= col)
        print("All election data processed and saved successfully.")

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
