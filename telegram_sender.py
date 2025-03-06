# 텔레그램 메시지 전송 모듈
import os
import pandas as pd
import telegram
import asyncio
import datetime
import time
import random
import logging
import glob
import json
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telegram_sender")

# .env 파일에서 환경변수 로드
load_dotenv()

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 최소 할인율 설정 (이 이상 할인된 상품만 알림)
MIN_DISCOUNT = int(os.getenv("MIN_DISCOUNT", "20"))  # 기본값 20% 이상 할인된 상품만

# 이전에 전송한 상품 기록 관리
SENT_RECORD_FILE = "data/sent_products.json"
GITHUB_SENT_RECORD_URL = "https://raw.githubusercontent.com/username/repo/main/data/sent_products.json"

def load_sent_products():
    """이전에 전송한 상품 목록 불러오기"""
    sent_products = {"sent_links": [], "last_update": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    
    # 로컬 파일 확인
    if os.path.exists(SENT_RECORD_FILE):
        try:
            with open(SENT_RECORD_FILE, 'r', encoding='utf-8') as f:
                local_data = json.load(f)
                if isinstance(local_data, dict) and "sent_links" in local_data:
                    sent_products = local_data
                    logger.info(f"로컬 전송 기록 파일 로드: {len(sent_products['sent_links'])}개 링크")
        except Exception as e:
            logger.error(f"로컬 전송 기록 파일 읽기 오류: {e}")
    
    # GitHub Actions 환경에서 실행 중인지 확인
    if os.environ.get("GITHUB_ACTIONS") == "true" and not os.path.exists(SENT_RECORD_FILE):
        try:
            import requests
            response = requests.get(GITHUB_SENT_RECORD_URL)
            if response.status_code == 200:
                github_data = response.json()
                if isinstance(github_data, dict) and "sent_links" in github_data:
                    # 로컬 데이터와 GitHub 데이터 병합
                    github_links = set(github_data["sent_links"])
                    local_links = set(sent_products["sent_links"])
                    all_links = list(github_links.union(local_links))
                    
                    sent_products["sent_links"] = all_links
                    logger.info(f"GitHub 전송 기록 파일 로드: {len(github_links)}개 링크")
        except Exception as e:
            logger.error(f"GitHub 전송 기록 파일 읽기 오류: {e}")
    
    return sent_products

def save_sent_products(sent_products):
    """전송한 상품 목록 저장"""
    try:
        os.makedirs(os.path.dirname(SENT_RECORD_FILE), exist_ok=True)
        
        # 중복 제거 (set 변환 후 다시 list로)
        sent_products["sent_links"] = list(set(sent_products["sent_links"]))
        
        with open(SENT_RECORD_FILE, 'w', encoding='utf-8') as f:
            json.dump(sent_products, f, ensure_ascii=False, indent=2)
            
        logger.info(f"전송 기록 파일 저장 완료: {len(sent_products['sent_links'])}개 링크")
    except Exception as e:
        logger.error(f"전송 기록 파일 저장 오류: {e}")

async def send_deal_message(bot, deal, retry_count=3):
    """개별 핫딜 상품 메시지 전송"""
    
    # 할인율이 최소 기준 미만이면 전송 안함
    if deal["discount"] < MIN_DISCOUNT:
        logger.info(f"할인율 부족으로 전송 제외: {deal['title'][:30]}... ({deal['discount']}%)")
        return False
    
    # 원래 가격과 현재 가격 숫자 형식 확인 및 변환
    try:
        original_price = int(deal["original_price"]) if isinstance(deal["original_price"], str) else deal["original_price"]
        price = int(deal["price"]) if isinstance(deal["price"], str) else deal["price"]
    except (ValueError, TypeError):
        original_price = 0
        price = 0
        logger.warning(f"가격 형식 오류: {deal['title']}")
    
    # 카테고리 확인
    category = deal.get("category", "일반")
    
    # 메시지 생성
    message = f"""🔥 <b>{deal['title']}</b>

💰 <b>{price:,}원</b> (원가: {original_price:,}원)
🏷️ <b>{deal['discount']}% 할인</b>
📁 {category}

🔗 <a href="{deal['link']}">구매 링크</a>
"""
    
    for attempt in range(retry_count):
        try:
            # 메시지 전송
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=False
            )
            logger.info(f"메시지 전송 성공: {deal['title'][:30]}...")
            return True
        
        except telegram.error.RetryAfter as e:
            # 텔레그램 rate limit 오류 처리
            wait_time = e.retry_after + 1
            logger.warning(f"텔레그램 rate limit (재시도 {attempt+1}/{retry_count}): {wait_time}초 대기")
            time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"메시지 전송 중 오류 (재시도 {attempt+1}/{retry_count}): {e}")
            time.sleep(3)  # 잠시 대기 후 재시도
    
    logger.error(f"최대 재시도 횟수 초과. 메시지 전송 실패: {deal['title']}")
    return False

async def send_image_message(bot, deal, retry_count=3):
    """이미지와 함께 핫딜 상품 메시지 전송"""
    
    # 할인율이 최소 기준 미만이면 전송 안함
    if deal["discount"] < MIN_DISCOUNT:
        return False
    
    # 이미지 URL이 없는 경우 텍스트만 전송
    if not deal.get("image_url"):
        return await send_deal_message(bot, deal, retry_count)
    
    # 원래 가격과 현재 가격 형식 변환
    try:
        original_price = int(deal["original_price"]) if isinstance(deal["original_price"], str) else deal["original_price"]
        price = int(deal["price"]) if isinstance(deal["price"], str) else deal["price"]
    except (ValueError, TypeError):
        original_price = 0
        price = 0
    
    # 카테고리 확인
    category = deal.get("category", "일반")
    
    # 캡션 생성
    caption = f"""🔥 <b>{deal['title']}</b>

💰 <b>{price:,}원</b> (원가: {original_price:,}원)
🏷️ <b>{deal['discount']}% 할인</b>
📁 {category}

🔗 <a href="{deal['link']}">구매 링크</a>
"""
    
    for attempt in range(retry_count):
        try:
            # 이미지와 함께 메시지 전송
            await bot.send_photo(
                chat_id=TELEGRAM_CHAT_ID,
                photo=deal["image_url"],
                caption=caption,
                parse_mode="HTML"
            )
            logger.info(f"이미지 메시지 전송 성공: {deal['title'][:30]}...")
            return True
        
        except telegram.error.BadRequest:
            # 이미지 URL 오류 시 텍스트만 전송
            logger.warning(f"이미지 URL 오류, 텍스트만 전송: {deal['title']}")
            return await send_deal_message(bot, deal)
        
        except telegram.error.RetryAfter as e:
            # 텔레그램 rate limit 오류 처리
            wait_time = e.retry_after + 1
            logger.warning(f"텔레그램 rate limit (재시도 {attempt+1}/{retry_count}): {wait_time}초 대기")
            time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"이미지 메시지 전송 중 오류 (재시도 {attempt+1}/{retry_count}): {e}")
            if attempt == retry_count - 1:
                # 마지막 시도에서는 텍스트만 전송 시도
                logger.info(f"이미지 전송 실패, 텍스트만 전송 시도: {deal['title']}")
                return await send_deal_message(bot, deal)
            time.sleep(3)  # 잠시 대기 후 재시도
    
    return False

async def send_top_deals(deals, max_items=10, use_images=True):
    """최근 수집한 핫딜 중 상위 N개 알림 전송"""
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("텔레그램 설정이 없습니다. .env 파일을 확인하세요.")
        return 0
    
    # 이전에 전송한 상품 기록 불러오기
    sent_products = load_sent_products()
    sent_links = set(sent_products["sent_links"])
    
    # 최근 전송 기록 정리 (최대 1000개 유지)
    if len(sent_links) > 1000:
        sent_products["sent_links"] = list(sent_links)[-1000:]
        sent_links = set(sent_products["sent_links"])
        logger.info(f"전송 기록 정리: 1000개로 제한 (원래: {len(sent_links)}개)")
    
    # 봇 객체 생성
    bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
    
    # 할인율 기준으로 정렬
    sorted_deals = sorted(deals, key=lambda x: x["discount"], reverse=True)
    
    # 이미 전송한 상품 제외 및 최소 할인율 필터링
    new_deals = []
    filtered_count = 0
    
    for deal in sorted_deals:
        # 링크 정규화 (쿼리 파라미터 제거)
        link = deal["link"].split("?")[0] if "?" in deal["link"] else deal["link"]
        
        # 이미 전송한 링크인지 확인 (정규화된 링크로 비교)
        if link in sent_links:
            filtered_count += 1
            continue
            
        # 최소 할인율 확인
        if deal["discount"] >= MIN_DISCOUNT:
            new_deals.append(deal)
            # 중복 전송 방지를 위해 링크 추가 (정규화된 링크 저장)
            sent_links.add(link)
            sent_products["sent_links"].append(link)
    
    logger.info(f"중복 필터링: {filtered_count}개 상품 제외됨")
    
    # 최대 개수 제한
    new_deals = new_deals[:max_items]
    
    if not new_deals:
        logger.info(f"전송할 새로운 핫딜이 없습니다. (최소 할인율: {MIN_DISCOUNT}%)")
        # 전송 기록 저장 (필터링된 링크 포함)
        sent_products["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_sent_products(sent_products)
        return 0
    
    # 헤더 메시지 전송
    today = datetime.datetime.now().strftime("%Y년 %m월 %d일 %H시")
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"📢 <b>{today} 쿠팡 핫딜 TOP {len(new_deals)}</b>",
            parse_mode="HTML"
        )
        logger.info(f"헤더 메시지 전송 완료")
    except Exception as e:
        logger.error(f"헤더 메시지 전송 중 오류: {e}")
    
    # 개별 상품 메시지 전송
    sent_count = 0
    for deal in new_deals:
        try:
            if use_images and deal.get("image_url"):
                success = await send_image_message(bot, deal)
            else:
                success = await send_deal_message(bot, deal)
                
            if success:
                sent_count += 1
            
            # 너무 많은 메시지를 한꺼번에 보내지 않도록 대기
            await asyncio.sleep(random.uniform(1, 2))
        except Exception as e:
            logger.error(f"상품 메시지 전송 중 오류: {e}")
    
    # 푸터 메시지
    if sent_count > 0:
        try:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text="위 상품들은 재고 소진 시 종료될 수 있습니다. 더 많은 핫딜은 채널에서 확인하세요!",
                parse_mode="HTML"
            )
            logger.info("푸터 메시지 전송 완료")
        except Exception as e:
            logger.error(f"푸터 메시지 전송 중 오류: {e}")
    
    # 전송 기록 저장
    sent_products["last_update"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_sent_products(sent_products)
    
    logger.info(f"텔레그램 전송 완료! 중복 제거 후 {sent_count}개 상품 전송")
    return sent_count

def find_latest_deals_file():
    """가장 최근에 생성된 핫딜 CSV 파일 찾기"""
    
    # data 디렉토리 내 모든 CSV 파일 검색
    csv_files = glob.glob("data/coupang_deals_*.csv")
    
    if not csv_files:
        logger.error("데이터 파일을 찾을 수 없습니다.")
        return None
    
    # 파일 생성 시간 기준으로 정렬
    latest_file = max(csv_files, key=os.path.getmtime)
    logger.info(f"최근 CSV 파일: {latest_file}")
    
    return latest_file

def main():
    """메인 함수: 최근 크롤링한 핫딜 정보를 텔레그램으로 전송"""
    
    logger.info("=== 텔레그램 핫딜 알림 전송 시작 ===")
    
    # 환경변수 확인
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("오류: 텔레그램 설정이 없습니다. .env 파일을 확인하세요.")
        return
    
    # 최근 파일 찾기
    file_path = find_latest_deals_file()
    
    if not file_path:
        logger.error("오류: 수집한 데이터를 찾을 수 없습니다.")
        return
    
    try:
        # 데이터 읽기
        df = pd.read_csv(file_path)
        logger.info(f"파일 '{file_path}'에서 {len(df)}개 상품 정보를 읽었습니다.")
        
        # 데이터프레임을 딕셔너리 리스트로 변환
        deals = df.to_dict('records')
        
        # 할인율 확인
        discount_stats = df['discount'].describe()
        logger.info(f"할인율 통계: 평균={discount_stats['mean']:.2f}%, 최대={discount_stats['max']}%")
        
        # 최소 할인율 이상인 상품 수
        eligible_count = len(df[df['discount'] >= MIN_DISCOUNT])
        logger.info(f"{MIN_DISCOUNT}% 이상 할인된 상품: {eligible_count}개")
        
        # 비동기 함수 실행
        sent_count = asyncio.run(send_top_deals(deals))
        logger.info(f"텔레그램 전송 완료: {sent_count}개 상품")
        
    except Exception as e:
        logger.error(f"데이터 처리 및 전송 중 오류: {e}", exc_info=True)
    
    logger.info("=== 텔레그램 핫딜 알림 전송 종료 ===")

if __name__ == "__main__":
    main()
