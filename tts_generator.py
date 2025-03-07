#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import time
import glob
import logging
import datetime
import requests
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tts_generator.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TTS_Generator")

def get_latest_script_file(directory="output"):
    """가장 최근에 생성된 스크립트 파일 경로 반환"""
    pattern = os.path.join(directory, "hotdeal_script_*.txt")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"'{pattern}' 패턴과 일치하는 파일을 찾을 수 없습니다.")
    return max(files, key=os.path.getctime)

def read_script_file(file_path):
    """스크립트 파일 내용 읽기"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def split_script_by_category(script):
    """스크립트를 카테고리별로 분리"""
    # 인트로 부분 추출 (첫 번째 카테고리 인트로 전까지)
    intro_pattern = r"^(.*?)(?=다음은|오늘의)"
    intro_match = re.search(intro_pattern, script, re.DOTALL)
    intro = intro_match.group(1).strip() if intro_match else ""
    
    # 아웃트로 부분 추출 (마지막 문단)
    outro_pattern = r"이상 오늘의 핫딜 정보였습니다.*$"
    outro_match = re.search(outro_pattern, script, re.DOTALL)
    outro = outro_match.group(0).strip() if outro_match else ""
    
    # 카테고리별 내용 추출
    # 카테고리 인트로 패턴 (예: "다음은 오늘의 식품 핫딜입니다." 또는 "오늘의 생활용품 핫딜을 소개합니다.")
    category_pattern = r"((?:다음은|오늘의).*?핫딜.*?(?:입니다|소개합니다).*?)(?=(?:다음은|오늘의)|이상 오늘의 핫딜 정보였습니다)"
    categories = re.findall(category_pattern, script, re.DOTALL)
    
    # 결과 정리
    result = {
        "intro": intro,
        "categories": [cat.strip() for cat in categories],
        "outro": outro
    }
    
    return result

def apply_pronunciation_corrections(text):
    """발음 교정 적용"""
    corrections = {
        "정가": "정까",
        "할인율": "할인율",
        "구매하실": "구매하실",
        "%": "퍼센트",
        "원에서": "원에서",
        "원": "원",
    }
    
    for original, corrected in corrections.items():
        text = text.replace(original, corrected)
    
    return text

def generate_voice_clova(text, output_path, speaker="vhyeri", volume=0, pitch=0, speed=1.1, retry_count=3, retry_delay=2):
    """네이버 Clova Voice로 음성 생성"""
    # API 키 가져오기
    api_key_id = os.environ.get('NAVER_API_KEY_ID')
    api_secret = os.environ.get('NAVER_API_SECRET')
    
    if not api_key_id or not api_secret:
        logger.error("네이버 API 키가 설정되지 않았습니다. 환경 변수 NAVER_API_KEY_ID와 NAVER_API_SECRET를 설정하세요.")
        return False
    
    url = "https://naveropenapi.apigw.ntruss.com/tts-premium/v1/tts"
    
    headers = {
        "X-NCP-APIGW-API-KEY-ID": api_key_id,
        "X-NCP-APIGW-API-KEY": api_secret,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # 발음 교정 적용
    text = apply_pronunciation_corrections(text)
    
    data = {
        "speaker": speaker,
        "volume": volume,
        "speed": speed,
        "pitch": pitch,
        "text": text,
        "format": "mp3"
    }
    
    for attempt in range(retry_count):
        try:
            response = requests.post(url, headers=headers, data=data)
            
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                logger.info(f"음성 파일 생성 완료: {output_path}")
                return True
            else:
                logger.warning(f"API 호출 실패 (시도 {attempt+1}/{retry_count}): {response.status_code}, {response.text}")
                if attempt < retry_count - 1:
                    logger.info(f"{retry_delay}초 후 재시도합니다...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"최대 재시도 횟수 초과. 음성 변환 실패: {output_path}")
                    return False
        except Exception as e:
            logger.error(f"예상치 못한 오류 발생: {e}")
            if attempt < retry_count - 1:
                logger.info(f"{retry_delay}초 후 재시도합니다...")
                time.sleep(retry_delay)
            else:
                return False
    
    return False

def generate_audio_files(script_parts, output_dir="audio", speaker="vhyeri", speed=1.1, pitch=0, volume=0):
    """스크립트 부분별로 음성 파일 생성"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 출력 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"출력 디렉토리 생성: {output_dir}")
    
    generated_files = []
    
    # 인트로 음성 생성
    if script_parts["intro"]:
        intro_file = os.path.join(output_dir, f"{timestamp}_00_intro.mp3")
        if generate_voice_clova(script_parts["intro"], intro_file, speaker, volume, pitch, speed):
            generated_files.append(intro_file)
    
    # 카테고리별 음성 생성
    for i, category_text in enumerate(script_parts["categories"], 1):
        category_file = os.path.join(output_dir, f"{timestamp}_{i:02d}_category.mp3")
        if generate_voice_clova(category_text, category_file, speaker, volume, pitch, speed):
            generated_files.append(category_file)
    
    # 아웃트로 음성 생성
    if script_parts["outro"]:
        outro_file = os.path.join(output_dir, f"{timestamp}_{len(script_parts['categories'])+1:02d}_outro.mp3")
        if generate_voice_clova(script_parts["outro"], outro_file, speaker, volume, pitch, speed):
            generated_files.append(outro_file)
    
    return generated_files

def main():
    try:
        # 환경 변수에서 설정 가져오기 (기본값 제공)
        speaker = os.environ.get('TTS_SPEAKER', 'vhyeri')  # 또는 'vminji'
        speed = float(os.environ.get('TTS_SPEED', '1.1'))  # 약간 빠르게
        pitch = int(os.environ.get('TTS_PITCH', '0'))
        volume = int(os.environ.get('TTS_VOLUME', '0'))
        output_dir = os.environ.get('TTS_OUTPUT_DIR', 'audio')
        
        # API 키 확인
        if not os.environ.get('NAVER_API_KEY_ID') or not os.environ.get('NAVER_API_SECRET'):
            logger.error("네이버 API 키가 설정되지 않았습니다. 환경 변수 NAVER_API_KEY_ID와 NAVER_API_SECRET를 설정하세요.")
            return
        
        # 최신 스크립트 파일 찾기
        script_file = get_latest_script_file()
        logger.info(f"최신 스크립트 파일: {script_file}")
        
        # 스크립트 파일 읽기
        script_content = read_script_file(script_file)
        logger.info(f"스크립트 내용 읽기 완료 ({len(script_content)} 바이트)")
        
        # 스크립트를 카테고리별로 분리
        script_parts = split_script_by_category(script_content)
        logger.info(f"스크립트 분리 완료: 인트로, {len(script_parts['categories'])}개 카테고리, 아웃트로")
        
        # 음성 파일 생성
        generated_files = generate_audio_files(
            script_parts, 
            output_dir=output_dir,
            speaker=speaker,
            speed=speed,
            pitch=pitch,
            volume=volume
        )
        logger.info(f"총 {len(generated_files)}개의 음성 파일 생성 완료")
        
        # 생성된 파일 목록 출력
        for file in generated_files:
            logger.info(f"생성된 파일: {file}")
        
    except FileNotFoundError as e:
        logger.error(f"파일을 찾을 수 없습니다: {e}")
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main()
