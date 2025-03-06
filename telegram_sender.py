# 텔레그램 메시지 전송 모듈
import os
import pandas as pd
import telegram
import asyncio
import datetime
from dotenv import load_dotenv
import time
import random

# .env 파일에서 환경변수 로드
load_dotenv()

# 텔레그램 설정
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# 최소 할인율 설정 (이 이상 할인된 상품만 알림)
MIN_DISCOUNT = 20  # 20% 이상 할인된 상품만

async def send_deal_message(bot, deal):
    """개별 핫딜 상품 메시지 전송"""
    
    # 할인율이 최소 기준 미만이면 전송 안함
    if deal["discount"] < MIN_DISCOUNT:
        return False
    
    # 메시지 생성
    message = f"""🔥 <b>{deal['title']}</b>

💰 <b>{deal['price']:,}원</b> (원가: {deal['original_price']:,}원)
🏷️ <b>{deal['discount']}% 할인</b>

📁 {deal['category']}
🔗 <a href="{deal['link']}">구매 링크</a>
"""
    
    try:
        # 메시지 전송
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=telegram.constants.ParseMode.HTML,
            disable_web_page_preview=False
        )
        return True
    except Exception as e:
        print(f"메시지 전송 중 오류: {e}")
        return False

async def send_top_deals(deals, max_items=5):
    """최근 수집한 핫딜 중 상위 N개 알림 전송"""
    
    # 봇 객체 생성
    bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
    
    # 할인율 기준으로 정렬
    sorted_deals = sorted(deals, key=lambda x: x["discount"], reverse=True)
    
    # 상위 N개 또는 20% 이상 할인된 모든 상품
    filtered_deals = [d for d in sorted_deals if d["discount"] >= MIN_DISCOUNT][:max_items]
    
    if not filtered_deals:
        print("전송할 핫딜이 없습니다.")
        return 0
    
    # 헤더 메시지 전송
    today = datetime.datetime.now().strftime("%Y년 %m월 %d일 %H시")
    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=f"📢 <b>{today} 쿠팡 핫딜 TOP {len(filtered_deals)}</b>",
        parse_mode=telegram.constants.ParseMode.HTML
    )
    
    # 개별 상품 메시지 전송
    sent_count = 0
    for deal in filtered_deals:
        success = await send_deal_message(bot, deal)
        if success:
            sent_count += 1
        
        # 너무 많은 메시지를 한꺼번에 보내지 않도록 대기
        time.sleep(random.uniform(1, 2))
    
    # 푸터 메시지
    if sent_count > 0:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="위 상품들은 재고 소진 시 종료될 수 있습니다.",
            parse_mode=telegram.constants.ParseMode.HTML
        )
    
    return sent_count

def main():
    """메인 함수: 최근 크롤링한 핫딜 정보를 텔레그램으로 전송"""
    
    # 환경변수 확인
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("오류: 텔레그램 설정이 없습니다. .env 파일을 확인하세요.")
        return
    
    # 최근 파일 찾기
    today = datetime.datetime.now().strftime("%Y%m%d")
    file_path = f"data/coupang_deals_{today}.csv"
    
    if not os.path.exists(file_path):
        print(f"오류: 오늘 수집한 데이터를 찾을 수 없습니다. 경로: {file_path}")
        return
    
    # 데이터 읽기
    df = pd.read_csv(file_path)
    print(f"파일 '{file_path}'에서 {len(df)}개 상품 정보를 읽었습니다.")
    
    # 데이터프레임을 딕셔너리 리스트로 변환
    deals = df.to_dict('records')
    
    # 비동기 함수 실행
    sent_count = asyncio.run(send_top_deals(deals))
    print(f"텔레그램 전송 완료: {sent_count}개 상품")

if __name__ == "__main__":
    main()
