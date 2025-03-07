#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import glob
import time
import logging
import datetime
import pickle
import requests
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("youtube_uploader.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("YouTubeUploader")

# 상수 정의
SCOPES = ['https://www.googleapis.com/auth/youtube.upload',
          'https://www.googleapis.com/auth/youtube']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
DEFAULT_CATEGORY_ID = '22'  # 사람 및 블로그 카테고리
DEFAULT_PRIVACY_STATUS = 'public'  # 기본 공개 상태 (public, private, unlisted)
DEFAULT_TAGS = ['핫딜', '쿠팡', '할인', '특가', '쇼핑', '추천', '리뷰']
HISTORY_FILE = 'upload_history.json'

def get_authenticated_service():
    """YouTube API 인증 및 서비스 객체 생성"""
    # 환경 변수에서 인증 정보 경로 가져오기
    credentials_path = os.environ.get('YOUTUBE_CREDENTIALS_PATH', 'credentials.json')
    token_path = os.environ.get('YOUTUBE_TOKEN_PATH', 'token.pickle')
    
    credentials = None
    
    # 토큰 파일이 있으면 로드
    if os.path.exists(token_path):
        logger.info(f"기존 토큰 파일 로드: {token_path}")
        with open(token_path, 'rb') as token:
            credentials = pickle.load(token)
    
    # 토큰이 없거나 유효하지 않으면 새로 인증
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            logger.info("토큰 갱신 중...")
            credentials.refresh(Request())
        else:
            logger.info("새로운 인증 흐름 시작...")
            if not os.path.exists(credentials_path):
                logger.error(f"인증 정보 파일을 찾을 수 없습니다: {credentials_path}")
                return None
                
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            credentials = flow.run_local_server(port=0)
        
        # 토큰 저장
        with open(token_path, 'wb') as token:
            pickle.dump(credentials, token)
            logger.info(f"토큰 저장 완료: {token_path}")
    
    # YouTube API 서비스 객체 생성
    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
        logger.info("YouTube API 서비스 객체 생성 완료")
        return service
    except Exception as e:
        logger.error(f"YouTube API 서비스 객체 생성 실패: {e}")
        return None

def get_latest_video_file(directory="output"):
    """최신 렌더링된 영상 파일 경로 반환"""
    pattern = os.path.join(directory, "hotdeal_video_*.mp4")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"'{pattern}' 패턴과 일치하는 파일을 찾을 수 없습니다.")
    return max(files, key=os.path.getctime)

def get_video_metadata(video_path):
    """영상 파일에서 메타데이터 추출"""
    # 파일명에서 타임스탬프 추출 (예: hotdeal_video_20230101_120000.mp4)
    filename = os.path.basename(video_path)
    parts = filename.split('_')
    
    timestamp = None
    if len(parts) >= 3:
        try:
            # 타임스탬프 형식: YYYYMMDD_HHMMSS
            timestamp_str = f"{parts[2]}_{parts[3].split('.')[0]}"
            timestamp = datetime.datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except (IndexError, ValueError) as e:
            logger.warning(f"파일명에서 타임스탬프 추출 실패: {e}")
    
    # 타임스탬프를 추출하지 못한 경우 현재 시간 사용
    if not timestamp:
        timestamp = datetime.datetime.now()
    
    # 메타데이터 파일 경로
    frames_dir = os.environ.get('FRAMES_DIR', 'frames')
    metadata_dir = os.path.join(frames_dir, "metadata")
    metadata_pattern = os.path.join(metadata_dir, f"{timestamp.strftime('%Y%m%d_%H%M%S')}_frames_info.json")
    
    categories = []
    products = []
    
    # 메타데이터 파일이 있으면 로드
    try:
        metadata_files = glob.glob(metadata_pattern)
        if metadata_files:
            with open(metadata_files[0], 'r', encoding='utf-8') as f:
                frames_info = json.load(f)
                
                # 카테고리 및 상품 정보 추출
                for frame in frames_info:
                    metadata = frame.get('metadata', {})
                    frame_type = metadata.get('type', '')
                    
                    if frame_type == 'category_intro':
                        category = metadata.get('category', '')
                        if category and category not in categories:
                            categories.append(category)
                    
                    elif frame_type == 'product':
                        product_title = metadata.get('product_title', '')
                        if product_title:
                            products.append(product_title)
    except Exception as e:
        logger.warning(f"메타데이터 파일 로드 실패: {e}")
    
    return {
        'timestamp': timestamp,
        'categories': categories,
        'products': products
    }

def generate_video_title(metadata):
    """영상 제목 생성"""
    timestamp = metadata.get('timestamp', datetime.datetime.now())
    formatted_date = timestamp.strftime("%Y년 %m월 %d일")
    
    return f"오늘의 쿠팡 핫딜 - {formatted_date}"

def generate_video_description(metadata):
    """영상 설명 생성"""
    timestamp = metadata.get('timestamp', datetime.datetime.now())
    formatted_date = timestamp.strftime("%Y년 %m월 %d일")
    categories = metadata.get('categories', [])
    products = metadata.get('products', [])
    
    description = f"📅 {formatted_date} 쿠팡 핫딜 정보를 소개합니다.\n\n"
    
    # 카테고리 정보 추가
    if categories:
        description += "🔍 오늘의 핫딜 카테고리\n"
        for i, category in enumerate(categories, 1):
            description += f"#{i} {category}\n"
        description += "\n"
    
    # 상품 정보 추가 (최대 10개)
    if products:
        description += "💰 주요 할인 상품\n"
        for i, product in enumerate(products[:10], 1):
            description += f"- {product}\n"
        
        if len(products) > 10:
            description += f"외 {len(products) - 10}개 상품\n"
        
        description += "\n"
    
    # 면책 조항 추가
    description += "⚠️ 주의사항\n"
    description += "- 가격 및 할인율은 변동될 수 있으니 구매 전 최종 가격을 확인하세요.\n"
    description += "- 재고 상황에 따라 상품이 품절될 수 있습니다.\n\n"
    
    # 채널 정보 추가
    description += "🔔 매일 업데이트되는 핫딜 정보를 받아보세요!\n"
    description += "구독과 좋아요, 알림 설정 부탁드립니다.\n\n"
    
    # 해시태그 추가
    description += "#핫딜 #쿠팡 #할인 #쇼핑 #특가 "
    if categories:
        for category in categories[:5]:  # 최대 5개 카테고리만 해시태그로 추가
            description += f"#{category.replace('/', '')} "
    
    return description

def generate_video_tags(metadata):
    """영상 태그 생성"""
    categories = metadata.get('categories', [])
    
    # 기본 태그
    tags = DEFAULT_TAGS.copy()
    
    # 카테고리 태그 추가
    for category in categories:
        # 태그에 특수문자 제거
        clean_category = ''.join(c for c in category if c.isalnum() or c.isspace())
        tags.append(clean_category)
    
    # 날짜 태그 추가
    timestamp = metadata.get('timestamp', datetime.datetime.now())
    tags.append(timestamp.strftime("%Y년%m월%d일"))
    tags.append(timestamp.strftime("%Y%m%d"))
    
    return tags

def initialize_upload(youtube, video_path, metadata):
    """YouTube에 영상 업로드 초기화"""
    # 환경 변수에서 설정 가져오기
    category_id = os.environ.get('YOUTUBE_CATEGORY_ID', DEFAULT_CATEGORY_ID)
    privacy_status = os.environ.get('YOUTUBE_PRIVACY_STATUS', DEFAULT_PRIVACY_STATUS)
    
    # 메타데이터 생성
    title = generate_video_title(metadata)
    description = generate_video_description(metadata)
    tags = generate_video_tags(metadata)
    
    # 공개 예약 설정
    scheduled_publish = os.environ.get('YOUTUBE_SCHEDULED_PUBLISH', 'False').lower() == 'true'
    publish_at = None
    
    if scheduled_publish:
        # 기본적으로 다음 날 오전 9시로 설정
        hours_offset = int(os.environ.get('YOUTUBE_PUBLISH_HOURS_OFFSET', '24'))
        publish_time = datetime.datetime.now() + datetime.timedelta(hours=hours_offset)
        publish_time = publish_time.replace(hour=9, minute=0, second=0, microsecond=0)
        publish_at = publish_time.isoformat() + 'Z'  # RFC 3339 형식
        privacy_status = 'private'  # 예약 업로드는 private으로 시작
    
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags,
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': privacy_status,
            'selfDeclaredMadeForKids': False
        }
    }
    
    # 공개 예약 설정이 있으면 추가
    if publish_at:
        body['status']['publishAt'] = publish_at
    
    logger.info(f"업로드 정보: 제목='{title}', 공개 상태={privacy_status}")
    if publish_at:
        logger.info(f"예약 공개 시간: {publish_at}")
    
    # 미디어 파일 설정
    media = MediaFileUpload(video_path, 
                           chunksize=1024*1024,
                           resumable=True)
    
    # 업로드 요청 초기화
    request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=media
    )
    
    return request

def upload_video(youtube, video_path, metadata, max_retries=3, retry_interval=5):
    """YouTube에 영상 업로드 및 진행 상황 모니터링"""
    request = initialize_upload(youtube, video_path, metadata)
    
    if not request:
        logger.error("업로드 요청 초기화 실패")
        return None
    
    video_id = None
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"영상 업로드 시작: {os.path.basename(video_path)}")
            
            response = None
            error = None
            
            # 업로드 진행 상황 모니터링
            while response is None:
                try:
                    status, response = request.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        logger.info(f"업로드 진행률: {progress}%")
                except Exception as e:
                    error = e
                    logger.warning(f"업로드 중 오류 발생: {e}")
                    break
            
            if error:
                retry_count += 1
                if retry_count < max_retries:
                    logger.info(f"{retry_interval}초 후 재시도 ({retry_count}/{max_retries})...")
                    time.sleep(retry_interval)
                    # 업로드 요청 재초기화
                    request = initialize_upload(youtube, video_path, metadata)
                    continue
                else:
                    logger.error(f"최대 재시도 횟수 초과. 업로드 실패")
                    return None
            
            if response:
                video_id = response.get('id')
                logger.info(f"업로드 완료! 비디오 ID: {video_id}")
                return video_id
        
        except Exception as e:
            logger.error(f"업로드 중 예상치 못한 오류: {e}")
            retry_count += 1
            if retry_count < max_retries:
                logger.info(f"{retry_interval}초 후 재시도 ({retry_count}/{max_retries})...")
                time.sleep(retry_interval)
            else:
                logger.error(f"최대 재시도 횟수 초과. 업로드 실패")
                return None
    
    return None

def update_upload_history(video_id, metadata, video_path):
    """업로드 히스토리 업데이트"""
    history = []
    
    # 기존 히스토리 파일이 있으면 로드
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception as e:
            logger.warning(f"히스토리 파일 로드 실패: {e}")
    
    # 새 업로드 정보 추가
    timestamp = metadata.get('timestamp', datetime.datetime.now())
    
    upload_info = {
        'video_id': video_id,
        'title': generate_video_title(metadata),
        'upload_time': datetime.datetime.now().isoformat(),
        'video_timestamp': timestamp.isoformat(),
        'video_path': video_path,
        'categories': metadata.get('categories', []),
        'product_count': len(metadata.get('products', []))
    }
    
    history.append(upload_info)
    
    # 히스토리 파일 저장
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info(f"업로드 히스토리 업데이트 완료: {HISTORY_FILE}")
    except Exception as e:
        logger.error(f"히스토리 파일 저장 실패: {e}")

def send_telegram_notification(video_id, metadata):
    """텔레그램으로 업로드 알림 전송"""
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        logger.warning("텔레그램 알림을 위한 환경 변수가 설정되지 않았습니다.")
        return False
    
    try:
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        title = generate_video_title(metadata)
        
        message = f"🎬 새 영상이 업로드되었습니다!\n\n"
        message += f"제목: {title}\n"
        message += f"링크: {video_url}\n\n"
        
        categories = metadata.get('categories', [])
        if categories:
            message += f"카테고리: {', '.join(categories[:3])}"
            if len(categories) > 3:
                message += f" 외 {len(categories) - 3}개"
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(url, data=payload)
        response.raise_for_status()
        
        logger.info("텔레그램 알림 전송 완료")
        return True
    
    except Exception as e:
        logger.error(f"텔레그램 알림 전송 실패: {e}")
        return False

def main():
    try:
        # YouTube API 인증
        youtube = get_authenticated_service()
        if not youtube:
            logger.error("YouTube API 인증 실패")
            return
        
        # 최신 영상 파일 찾기
        video_path = get_latest_video_file()
        logger.info(f"최신 영상 파일: {video_path}")
        
        # 메타데이터 추출
        metadata = get_video_metadata(video_path)
        logger.info(f"메타데이터 추출 완료: {len(metadata.get('categories', []))}개 카테고리, {len(metadata.get('products', []))}개 상품")
        
        # 영상 업로드
        video_id = upload_video(youtube, video_path, metadata)
        
        if video_id:
            # 업로드 히스토리 업데이트
            update_upload_history(video_id, metadata, video_path)
            
            # 텔레그램 알림 전송
            send_telegram_notification(video_id, metadata)
            
            logger.info(f"영상 업로드 및 후처리 완료: https://www.youtube.com/watch?v={video_id}")
        else:
            logger.error("영상 업로드 실패")
        
    except FileNotFoundError as e:
        logger.error(f"파일을 찾을 수 없습니다: {e}")
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main() 