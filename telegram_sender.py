#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
텔레그램 핫딜 전송 스크립트
수집된 쿠팡 핫딜 정보를 텔레그램 봇을 통해 전송합니다.
"""

import os
import json
import logging
import pandas as pd
import datetime
import time
import random
import telegram
import requests
import collections
from dotenv import load_dotenv
from pathlib import Path
from telegram.error import (
    TelegramError, BadRequest, TimedOut, 
    NetworkError, ChatMigrated, RetryAfter
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_sender.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_sender")

# 환경 변수 로드
load_dotenv()

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
MIN_DISCOUNT = int(os.environ.get('MIN_DISCOUNT', '20'))  # 기본값 20%
MAX_SEND_COUNT = int(os.environ.get('MAX_SEND_COUNT', '30'))  # 한 번에 최대 30개 전송
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))  # 전송 실패 시 최대 재시도 횟수
MAX_SENT_PRODUCTS = int(os.environ.get('MAX_SENT_PRODUCTS', '500'))  # 저장할 최대 전송 기록 수

# 이미 전송한 상품 목록 파일 경로
SENT_PRODUCTS_FILE = "data/sent_products.json"
BACKUP_SENT_PRODUCTS_FILE = "data/sent_products.backup.json"

def load_deals():
    """최신 핫딜 데이터 로드"""
    try:
        # data 폴더 내 가장 최신 CSV 파일 찾기
        data_dir = Path("data")
        if not data_dir.exists():
            logger.error("data 폴더가 존재하지 않습니다.")
            return None
        
        csv_files = list(data_dir.glob("coupang_deals_*.csv"))
        if not csv_files:
            logger.error("핫딜 데이터 파일을 찾을 수 없습니다.")
            return None
        
        # 파일명 기준으로 정렬하여 가장 최신 파일 선택
        latest_file = sorted(csv_files, reverse=True)[0]
        logger.info(f"최신 핫딜 데이터 파일: {latest_file}")
        
        # CSV 파일 로드
        df = pd.read_csv(latest_file)
        logger.info(f"총 {len(df)}개 상품 데이터 로드 완료")
        
        return df
    
    except Exception as e:
        logger.error(f"데이터 로드 중 오류 발생: {e}")
        return None

def load_sent_products():
    """이미 전송한 상품 목록 로드 (복구 메커니즘 포함)"""
    try:
        # data 폴더 생성
        os.makedirs(os.path.dirname(SENT_PRODUCTS_FILE), exist_ok=True)
        
        # 파일이 존재하면 로드
        if os.path.exists(SENT_PRODUCTS_FILE):
            try:
                with open(SENT_PRODUCTS_FILE, 'r', encoding='utf-8') as f:
                    sent_products = json.load(f)
                logger.info(f"전송 기록 로드 완료: {len(sent_products)} 상품")
                
                # 백업 파일 생성
                with open(BACKUP_SENT_PRODUCTS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(sent_products, f, ensure_ascii=False, indent=2)
                
                return sent_products
            except json.JSONDecodeError:
                logger.warning("전송 기록 파일이 손상되었습니다. 백업 파일을 확인합니다.")
                
                # 백업 파일이 있으면 복구 시도
                if os.path.exists(BACKUP_SENT_PRODUCTS_FILE):
                    try:
                        with open(BACKUP_SENT_PRODUCTS_FILE, 'r', encoding='utf-8') as f:
                            sent_products = json.load(f)
                        logger.info(f"백업 파일에서 전송 기록 복구 완료: {len(sent_products)} 상품")
                        
                        # 복구된 데이터 저장
                        with open(SENT_PRODUCTS_FILE, 'w', encoding='utf-8') as f:
                            json.dump(sent_products, f, ensure_ascii=False, indent=2)
                        
                        return sent_products
                    except Exception as e:
                        logger.error(f"백업 파일에서 복구 실패: {e}")
        
        # 파일이 없거나 복구 실패 시 빈 딕셔너리 반환
        logger.info("전송 기록 파일이 없거나 복구 실패. 새로 생성합니다.")
        return {}
    
    except Exception as e:
        logger.error(f"전송 기록 로드 중 오류 발생: {e}")
        return {}

def save_sent_products(sent_products):
    """전송한 상품 목록 저장 (최대 개수 제한)"""
    try:
        # 최대 개수 제한 (최신 항목 유지)
        if len(sent_products) > MAX_SENT_PRODUCTS:
            logger.info(f"전송 기록이 {len(sent_products)}개로 제한 ({MAX_SENT_PRODUCTS}개)을 초과하여 정리합니다.")
            
            # OrderedDict로 변환하여 최신 항목만 유지
            ordered_dict = collections.OrderedDict()
            
            # 날짜 기준으로 정렬 (최신순)
            sorted_items = sorted(
                sent_products.items(),
                key=lambda x: x[1].get('sent_date', '2000-01-01'),
                reverse=True
            )
            
            # 최대 개수만큼만 유지
            for i, (key, value) in enumerate(sorted_items):
                if i < MAX_SENT_PRODUCTS:
                    ordered_dict[key] = value
            
            sent_products = dict(ordered_dict)
            logger.info(f"전송 기록을 {len(sent_products)}개로 정리했습니다.")
        
        # 파일 저장
        with open(SENT_PRODUCTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(sent_products, f, ensure_ascii=False, indent=2)
        logger.info(f"전송 기록 저장 완료: {len(sent_products)} 상품")
        
        # 백업 파일도 업데이트
        with open(BACKUP_SENT_PRODUCTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(sent_products, f, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"전송 기록 저장 중 오류 발생: {e}")

def filter_deals(df, min_discount=MIN_DISCOUNT):
    """핫딜 필터링: 할인율 기준"""
    if df is None or len(df) == 0:
        return []
    
    # 할인율 기준으로 필터링
    filtered_df = df[df['discount'] >= min_discount].copy()
    
    # 할인율 기준으로 정렬
    filtered_df = filtered_df.sort_values(by='discount', ascending=False)
    
    logger.info(f"할인율 {min_discount}% 이상 상품: {len(filtered_df)}개")
    
    return filtered_df

def download_image(image_url, max_retries=MAX_RETRIES):
    """이미지 다운로드 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            response = requests.get(image_url, stream=True, timeout=10)
            if response.status_code == 200:
                return response.content
            else:
                logger.warning(f"이미지 다운로드 실패 ({attempt+1}/{max_retries}): HTTP {response.status_code} - {image_url}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # 재시도 전 대기
        except requests.exceptions.Timeout:
            logger.warning(f"이미지 다운로드 타임아웃 ({attempt+1}/{max_retries}): {image_url}")
            if attempt < max_retries - 1:
                time.sleep(1)
        except requests.exceptions.ConnectionError:
            logger.warning(f"이미지 다운로드 연결 오류 ({attempt+1}/{max_retries}): {image_url}")
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            logger.error(f"이미지 다운로드 중 예상치 못한 오류 ({attempt+1}/{max_retries}): {e} - {image_url}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    logger.error(f"이미지 다운로드 최종 실패: {image_url}")
    return None

def send_telegram_message(bot, chat_id, text=None, photo=None, parse_mode='HTML', max_retries=MAX_RETRIES):
    """텔레그램 메시지 전송 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            if photo:
                message = bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=text,
                    parse_mode=parse_mode
                )
            else:
                message = bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode,
                    disable_web_page_preview=False
                )
            return True, message
        
        except RetryAfter as e:
            # 텔레그램 API 제한에 걸린 경우 (초당 메시지 수 제한)
            retry_seconds = e.retry_after
            logger.warning(f"텔레그램 API 제한 ({attempt+1}/{max_retries}): {retry_seconds}초 후 재시도")
            if attempt < max_retries - 1:
                time.sleep(retry_seconds + 0.5)  # 여유있게 대기
        
        except BadRequest as e:
            # 잘못된 요청 (메시지 형식 오류 등)
            logger.error(f"텔레그램 BadRequest 오류 ({attempt+1}/{max_retries}): {e}")
            if "can't parse entities" in str(e).lower():
                # HTML/Markdown 파싱 오류인 경우 파싱 모드 없이 재시도
                logger.warning("파싱 모드 오류로 인해 일반 텍스트로 재시도합니다.")
                try:
                    if photo:
                        message = bot.send_photo(
                            chat_id=chat_id,
                            photo=photo,
                            caption=text,
                            parse_mode=None
                        )
                    else:
                        message = bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            parse_mode=None,
                            disable_web_page_preview=False
                        )
                    return True, message
                except Exception as inner_e:
                    logger.error(f"일반 텍스트로 재시도 중 오류: {inner_e}")
            return False, None
        
        except TimedOut as e:
            # 타임아웃
            logger.warning(f"텔레그램 타임아웃 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # 재시도 전 대기
        
        except NetworkError as e:
            # 네트워크 오류
            logger.warning(f"텔레그램 네트워크 오류 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(3)  # 재시도 전 대기
        
        except TelegramError as e:
            # 기타 텔레그램 오류
            logger.error(f"텔레그램 오류 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False, None
        
        except Exception as e:
            # 예상치 못한 오류
            logger.error(f"텔레그램 전송 중 예상치 못한 오류 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return False, None
    
    logger.error("텔레그램 전송 최종 실패")
    return False, None

def send_deal_to_telegram(bot, chat_id, deal, with_image=True):
    """텔레그램으로 핫딜 정보 전송"""
    try:
        # HTML 형식으로 메시지 생성
        message = f"🔥 <b>{deal['title']}</b>\n\n"
        message += f"💰 가격: {deal['price']:,}원 (원가: {deal['original_price']:,}원)\n"
        message += f"🏷️ 할인율: <b>{deal['discount']}%</b>\n"
        if 'category' in deal and deal['category']:
            message += f"📂 카테고리: {deal['category']}\n"
        message += f"🔗 <a href=\"{deal['link']}\">상품 링크</a>"
        
        # 이미지가 있고 with_image가 True인 경우 이미지와 함께 전송
        if with_image and deal.get('image_url'):
            image_data = download_image(deal['image_url'])
            if image_data:
                success, _ = send_telegram_message(
                    bot=bot,
                    chat_id=chat_id,
                    text=message,
                    photo=image_data,
                    parse_mode='HTML'
                )
                
                if success:
                    logger.info(f"이미지와 함께 전송 완료: {deal['title']}")
                    return True
                else:
                    logger.warning(f"이미지 전송 실패, 텍스트만 전송 시도: {deal['title']}")
        
        # 이미지가 없거나 다운로드 실패한 경우 텍스트만 전송
        success, _ = send_telegram_message(
            bot=bot,
            chat_id=chat_id,
            text=message,
            parse_mode='HTML'
        )
        
        if success:
            logger.info(f"텍스트로 전송 완료: {deal['title']}")
            return True
        else:
            logger.error(f"텍스트 전송도 실패: {deal['title']}")
            return False
    
    except Exception as e:
        logger.error(f"텔레그램 전송 중 오류 발생: {e}")
        return False

def main():
    """메인 함수: 핫딜 정보 필터링 및 텔레그램 전송"""
    logger.info("=== 텔레그램 핫딜 전송 시작 ===")
    
    # 환경 변수 확인
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("텔레그램 설정이 없습니다. 환경 변수를 확인하세요.")
        return
    
    # 텔레그램 봇 초기화
    try:
        bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        logger.info("텔레그램 봇 초기화 완료")
    except Exception as e:
        logger.error(f"텔레그램 봇 초기화 실패: {e}")
        return
    
    # 핫딜 데이터 로드
    df = load_deals()
    if df is None:
        logger.error("핫딜 데이터를 로드할 수 없습니다.")
        return
    
    # 이미 전송한 상품 목록 로드
    sent_products = load_sent_products()
    
    # 핫딜 필터링
    filtered_deals = filter_deals(df, min_discount=MIN_DISCOUNT)
    if len(filtered_deals) == 0:
        logger.info(f"전송할 핫딜이 없습니다. (할인율 {MIN_DISCOUNT}% 이상)")
        return
    
    # 현재 날짜 (전송 날짜 기록용)
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # 전송 카운터
    sent_count = 0
    already_sent_count = 0
    
    # 핫딜 전송 (최대 개수 제한)
    for _, deal in filtered_deals.iterrows():
        # 최대 전송 수 제한 확인
        if sent_count >= MAX_SEND_COUNT:
            logger.info(f"최대 전송 수 ({MAX_SEND_COUNT}개)에 도달하여 중단합니다.")
            break
        
        # 딕셔너리로 변환
        deal_dict = deal.to_dict()
        
        # 상품 ID (링크 기준)
        product_id = deal_dict['link']
        
        # 이미 전송한 상품인지 확인
        if product_id in sent_products:
            logger.info(f"이미 전송한 상품: {deal_dict['title']}")
            already_sent_count += 1
            continue
        
        # 텔레그램으로 전송
        success = send_deal_to_telegram(bot, TELEGRAM_CHAT_ID, deal_dict)
        
        if success:
            # 전송 기록 추가
            sent_products[product_id] = {
                "title": deal_dict['title'],
                "price": int(deal_dict['price']),
                "discount": int(deal_dict['discount']),
                "sent_date": today
            }
            sent_count += 1
            
            # 전송 기록 저장 (5개마다)
            if sent_count % 5 == 0:
                save_sent_products(sent_products)
            
            # 텔레그램 API 제한 방지를 위한 대기
            time.sleep(random.uniform(1.5, 3.0))
    
    # 최종 전송 기록 저장
    save_sent_products(sent_products)
    
    logger.info(f"총 {sent_count}개 상품 전송 완료 (이미 전송: {already_sent_count}개, 최대 제한: {MAX_SEND_COUNT}개)")
    logger.info("=== 텔레그램 핫딜 전송 종료 ===")

if __name__ == "__main__":
    main()
