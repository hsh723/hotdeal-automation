# 쿠팡 핫딜 크롤러 (BeautifulSoup 버전)
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import random
import os
import logging
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("coupang_crawler")

# .env 파일에서 환경변수 로드
load_dotenv()

# 크롤링 설정
MAX_PAGES = int(os.getenv("MAX_PAGES", "3"))
DELAY_MIN = float(os.getenv("DELAY_MIN", "3"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "5"))

# 다양한 User-Agent 목록
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

def get_random_user_agent():
    """랜덤 User-Agent 반환"""
    return random.choice(USER_AGENTS)

def get_headers():
    """HTTP 요청용 헤더 생성"""
    return {
        "User-Agent": get_random_user_agent(),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.coupang.com/"
    }

def get_coupang_deals(page=1, retries=3):
    """쿠팡 로켓 와우 핫딜 페이지에서 상품 정보 수집"""
    
    # 쿠팡 와우 핫딜 페이지 주소 (여러 URL 시도)
    urls = [
        f"https://www.coupang.com/np/campaigns/82/components/194176?page={page}",  # 원래 URL
        f"https://www.coupang.com/np/campaigns/82?page={page}",  # 대체 URL 1
        f"https://www.coupang.com/np/campaigns/82?componentId=194176&page={page}"  # 대체 URL 2
    ]
    
    session = requests.Session()
    
    for url in urls:
        logger.info(f"URL 요청: {url}")
        
        headers = get_headers()
        
        for attempt in range(retries):
            try:
                # 요청 간 딜레이 추가
                if attempt > 0:
                    time.sleep(random.uniform(2, 5))
                
                # 페이지 가져오기
                response = session.get(url, headers=headers, timeout=10)
                
                # 응답 확인
                logger.info(f"응답 상태 코드: {response.status_code}")
                
                if response.status_code == 200:
                    # HTML 저장 (디버깅용)
                    os.makedirs("debug", exist_ok=True)
                    with open(f"debug/coupang_page_{page}.html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                    
                    # HTML 파싱
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    # 각종 상품 목록 셀렉터 시도
                    item_selectors = [
                        "ul.productList li.baby-product",
                        "ul.baby-product-list li.baby-product",
                        "li.search-product",
                        "ul.products li.product-item"
                    ]
                    
                    items = []
                    for selector in item_selectors:
                        items = soup.select(selector)
                        if items:
                            logger.info(f"셀렉터 '{selector}'로 {len(items)}개 상품 찾음")
                            break
                    
                    # '와우 특가', '핫딜' 등의 텍스트가 있는지 확인
                    page_text = soup.get_text()
                    if "와우" in page_text and "특가" in page_text:
                        logger.info("페이지에 '와우 특가' 텍스트 존재")
                    if "핫딜" in page_text:
                        logger.info("페이지에 '핫딜' 텍스트 존재")
                    
                    if not items:
                        logger.warning("상품 목록을 찾을 수 없음")
                        continue  # 다음 URL 시도
                    
                    deals = []
                    for item in items:
                        # 상품 정보 추출
                        try:
                            # 다양한 제목 셀렉터 시도
                            title_selectors = ["div.name", "div.product-name"]
                            title = None
                            for selector in title_selectors:
                                title_elem = item.select_one(selector)
                                if title_elem:
                                    title = title_elem.text.strip()
                                    break
                            
                            if not title:
                                logger.warning("제목 요소를 찾을 수 없음")
                                continue
                            
                            # 다양한 가격 셀렉터 시도
                            price_selectors = ["strong.price-value", "strong.price", "em.sale"]
                            price = 0
                            for selector in price_selectors:
                                price_elem = item.select_one(selector)
                                if price_elem:
                                    price_text = price_elem.text.strip()
                                    price_text = ''.join(filter(str.isdigit, price_text))
                                    if price_text:
                                        price = int(price_text)
                                        break
                            
                            if price == 0:
                                logger.warning(f"가격 요소를 찾을 수 없음: {title}")
                                continue
                            
                            # 다양한 원래 가격 셀렉터 시도
                            original_price_selectors = ["del.base-price", "span.origin-price"]
                            original_price = price  # 기본값
                            for selector in original_price_selectors:
                                original_price_elem = item.select_one(selector)
                                if original_price_elem:
                                    original_price_text = original_price_elem.text.strip()
                                    original_price_text = ''.join(filter(str.isdigit, original_price_text))
                                    if original_price_text:
                                        original_price = int(original_price_text)
                                        break
                            
                            # 할인율 계산 또는 추출
                            discount_selectors = ["span.discount-rate", "span.discount-percentage"]
                            discount = 0
                            for selector in discount_selectors:
                                discount_elem = item.select_one(selector)
                                if discount_elem:
                                    discount_text = discount_elem.text.strip()
                                    discount_text = ''.join(filter(str.isdigit, discount_text))
                                    if discount_text:
                                        discount = int(discount_text)
                                        break
                            
                            # 할인율이 없으면 계산
                            if discount == 0 and original_price > price:
                                discount = round((original_price - price) / original_price * 100)
                            
                            # 링크 추출
                            link_selectors = ["a.baby-product-link", "a.product-link"]
                            link = ""
                            for selector in link_selectors:
                                link_elem = item.select_one(selector)
                                if link_elem and link_elem.has_attr("href"):
                                    link = "https://www.coupang.com" + link_elem["href"]
                                    break
                            
                            # 이미지 URL
                            img_selectors = ["img.product-image", "img.search-product-wrap-img"]
                            image_url = ""
                            for selector in img_selectors:
                                img_elem = item.select_one(selector)
                                if img_elem and img_elem.has_attr("src"):
                                    image_url = img_elem["src"]
                                    if not image_url.startswith("http"):
                                        image_url = "https:" + image_url
                                    break
                            
                            # 카테고리 (있는 경우)
                            category_selectors = ["div.category", "span.category"]
                            category = "일반"
                            for selector in category_selectors:
                                category_elem = item.select_one(selector)
                                if category_elem:
                                    category = category_elem.text.strip()
                                    break
                            
                            # 데이터 추가
                            deals.append({
                                "title": title,
                                "price": price,
                                "original_price": original_price,
                                "discount": discount,
                                "link": link,
                                "image_url": image_url,
                                "category": category,
                                "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                            
                        except Exception as e:
                            logger.error(f"상품 정보 추출 중 오류: {e}")
                    
                    if deals:
                        return deals
                
                else:
                    logger.warning(f"페이지 가져오기 실패: 상태 코드 {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"요청 오류 (재시도 {attempt+1}/{retries}): {e}")
        
        # 다음 URL 시도 전 잠시 대기
        time.sleep(random.uniform(1, 3))
    
    # 모든 URL 시도 실패
    logger.error("모든 URL에서 상품 정보를 가져오는 데 실패")
    return []

def get_goldbox_deals(retries=3):
    """쿠팡 골드박스 핫딜 페이지에서 상품 정보 수집"""
    
    url = "https://www.coupang.com/np/goldbox"
    logger.info(f"URL 요청: {url}")
    
    session = requests.Session()
    headers = get_headers()
    
    for attempt in range(retries):
        try:
            # 요청 간 딜레이 추가
            if attempt > 0:
                time.sleep(random.uniform(2, 5))
            
            # 페이지 가져오기
            response = session.get(url, headers=headers, timeout=10)
            
            # 응답 확인
            logger.info(f"응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                # HTML 저장 (디버깅용)
                os.makedirs("debug", exist_ok=True)
                with open("debug/coupang_goldbox.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                
                # HTML 파싱
                soup = BeautifulSoup(response.text, "html.parser")
                
                # 상품 목록 찾기
                item_selectors = [
                    "ul.goldbox-list li.product-item",
                    "div.product-item",
                    "ul.productList li.product-item"
                ]
                
                items = []
                for selector in item_selectors:
                    items = soup.select(selector)
                    if items:
                        logger.info(f"셀렉터 '{selector}'로 {len(items)}개 상품 찾음")
                        break
                
                if not items:
                    logger.warning("상품 목록을 찾을 수 없음")
                    continue  # 다음 시도
                
                deals = []
                for item in items:
                    # 상품 정보 추출
                    try:
                        # 상품명
                        title_selectors = ["div.name", "div.title"]
                        title = None
                        for selector in title_selectors:
                            title_elem = item.select_one(selector)
                            if title_elem:
                                title = title_elem.text.strip()
                                break
                        
                        if not title:
                            logger.warning("제목 요소를 찾을 수 없음")
                            continue
                        
                        # 현재 가격
                        price_selectors = ["strong.price-value", "strong.price"]
                        price = 0
                        for selector in price_selectors:
                            price_elem = item.select_one(selector)
                            if price_elem:
                                price_text = price_elem.text.strip()
                                price_text = ''.join(filter(str.isdigit, price_text))
                                if price_text:
                                    price = int(price_text)
                                    break
                        
                        if price == 0:
                            logger.warning(f"가격 요소를 찾을 수 없음: {title}")
                            continue
                        
                        # 원래 가격
                        original_price_selectors = ["span.base-price", "del.base-price"]
                        original_price = price  # 기본값
                        for selector in original_price_selectors:
                            original_price_elem = item.select_one(selector)
                            if original_price_elem:
                                original_price_text = original_price_elem.text.strip()
                                original_price_text = ''.join(filter(str.isdigit, original_price_text))
                                if original_price_text:
                                    original_price = int(original_price_text)
                                    break
                        
                        # 할인율
                        discount_selectors = ["span.discount-percentage", "span.discount-rate"]
                        discount = 0
                        for selector in discount_selectors:
                            discount_elem = item.select_one(selector)
                            if discount_elem:
                                discount_text = discount_elem.text.strip()
                                discount_text = ''.join(filter(str.isdigit, discount_text))
                                if discount_text:
                                    discount = int(discount_text)
                                    break
                        
                        # 할인율이 없으면 계산
                        if discount == 0 and original_price > price:
                            discount = round((original_price - price) / original_price * 100)
                        
                        # 링크
                        link_selectors = ["a.product-link", "a"]
                        link = ""
                        for selector in link_selectors:
                            link_elems = item.select(selector)
                            for elem in link_elems:
                                if elem.has_attr("href") and ("/vp/" in elem["href"] or "/np/goldbox" in elem["href"]):
                                    link = "https://www.coupang.com" + elem["href"]
                                    break
                            if link:
                                break
                        
                        # 이미지 URL
                        img_selectors = ["img.product-image", "img"]
                        image_url = ""
                        for selector in img_selectors:
                            img_elem = item.select_one(selector)
                            if img_elem and img_elem.has_attr("src"):
                                image_url = img_elem["src"]
                                if not image_url.startswith("http"):
                                    image_url = "https:" + image_url
                                break
                        
                        # 데이터 추가
                        deals.append({
                            "title": title,
                            "price": price,
                            "original_price": original_price,
                            "discount": discount,
                            "link": link,
                            "image_url": image_url,
                            "category": "골드박스",
                            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        
                    except Exception as e:
                        logger.error(f"상품 정보 추출 중 오류: {e}")
                
                if deals:
                    return deals
            
        except requests.exceptions.RequestException as e:
            logger.error(f"요청 오류 (재시도 {attempt+1}/{retries}): {e}")
    
    logger.error("골드박스 페이지에서 상품 정보를 가져오는 데 실패")
    return []

def try_deal_of_the_day(retries=3):
    """오늘의 핫딜 시도"""
    url = "https://www.coupang.com/np/campaigns/933"
    session = requests.Session()
    headers = get_headers()
    
    try:
        response = session.get(url, headers=headers, timeout=10)
        # HTML 저장 (디버깅용)
        os.makedirs("debug", exist_ok=True)
        with open("debug/coupang_deal_of_day.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info(f"오늘의 핫딜 페이지 상태 코드: {response.status_code}")
        # 이 페이지에 대한 크롤링 로직은 나중에 추가
    except Exception as e:
        logger.error(f"오늘의 핫딜 페이지 요청 중 오류: {e}")
    
    return []

def create_sample_deals():
    """크롤링 실패 시 샘플 데이터 생성"""
    logger.info("테스트용 샘플 데이터 생성")
    
    # 현실적인 샘플 데이터
    sample_deals = [
        {
            "title": "삼성전자 갤럭시 스마트폰 최신형 특가",
            "price": 899000,
            "original_price": 1250000,
            "discount": 28,
            "link": "https://www.coupang.com/sample/product1",
            "image_url": "https://thumbnail10.coupangcdn.com/thumbnails/sample_product1.jpg",
            "category": "전자제품",
            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        {
            "title": "Apple 에어팟 프로 2세대 MFI 케이블 증정",
            "price": 259000,
            "original_price": 359000,
            "discount": 31,
            "link": "https://www.coupang.com/sample/product2",
            "image_url": "https://thumbnail10.coupangcdn.com/thumbnails/sample_product2.jpg",
            "category": "디지털/가전",
            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        {
            "title": "다이슨 V11 무선청소기 특가할인",
            "price": 688000,
            "original_price": 899000,
            "discount": 23,
            "link": "https://www.coupang.com/sample/product3",
            "image_url": "https://thumbnail10.coupangcdn.com/thumbnails/sample_product3.jpg",
            "category": "생활가전",
            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        {
            "title": "나이키 에어포스1 07 로우 스니커즈 화이트",
            "price": 89000,
            "original_price": 129000,
            "discount": 31,
            "link": "https://www.coupang.com/sample/product4",
            "image_url": "https://thumbnail10.coupangcdn.com/thumbnails/sample_product4.jpg",
            "category": "패션의류",
            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        {
            "title": "LG 그램 17인치 노트북 최신형 i7",
            "price": 1499000,
            "original_price": 1899000,
            "discount": 21,
            "link": "https://www.coupang.com/sample/product5",
            "image_url": "https://thumbnail10.coupangcdn.com/thumbnails/sample_product5.jpg",
            "category": "노트북/PC",
            "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    ]
    
    return sample_deals

def save_deals_to_csv(deals, test_mode=False):
    """수집한 핫딜 정보를 CSV 파일로 저장"""
    
    if not deals:
        logger.warning("저장할 상품 정보가 없습니다.")
        return None
    
    # 데이터 저장 폴더 생성
    os.makedirs("data", exist_ok=True)
    
    # 오늘 날짜로 파일명 생성
    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"coupang_deals_{today}.csv"
    if test_mode:
        filename = f"coupang_deals_{today}_test.csv"
        
    file_path = os.path.join("data", filename)
    
    # DataFrame으로 변환 및 저장
    df = pd.DataFrame(deals)
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    logger.info(f"수집 완료: {len(deals)}개 상품, 저장 경로: {file_path}")
    
    return file_path

def main():
    """메인 함수: 여러 페이지의 핫딜 정보 수집"""
    
    logger.info("=== 쿠팡 핫딜 크롤링 시작 ===")
    
    all_deals = []
    
    # 골드박스 핫딜 수집
    logger.info("골드박스 핫딜 수집 중...")
    goldbox_deals = get_goldbox_deals()
    if goldbox_deals:
        all_deals.extend(goldbox_deals)
        logger.info(f"골드박스 핫딜 {len(goldbox_deals)}개 수집 완료")
    else:
        logger.warning("골드박스 핫딜 수집 실패")
    
    # 오늘의 핫딜 시도 (추가 소스)
    today_deals = try_deal_of_the_day()
    if today_deals:
        all_deals.extend(today_deals)
        logger.info(f"오늘의 핫딜 {len(today_deals)}개 수집 완료")
    
    # 일반 핫딜 페이지 수집
    for page in range(1, MAX_PAGES + 1):
        logger.info(f"핫딜 페이지 {page} 수집 중...")
        
        # 핫딜 정보 가져오기
        deals = get_coupang_deals(page=page)
        
        if deals:
            all_deals.extend(deals)
            logger.info(f"페이지 {page}에서 {len(deals)}개 상품 수집 완료")
        else:
            logger.warning(f"페이지 {page}에서 상품을 찾을 수 없습니다.")
        
        # IP 차단 방지를 위한 대기
        if page < MAX_PAGES:  # 마지막 페이지 이후에는 대기할 필요 없음
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            logger.info(f"다음 페이지 요청 전 {delay:.2f}초 대기 중...")
            time.sleep(delay)
    
    # 모든 수집 결과 처리
    if all_deals:
        # 중복 제거 (URL 기준)
        unique_deals = []
        seen_links = set()
        
        for deal in all_deals:
            if deal["link"] not in seen_links:
                seen_links.add(deal["link"])
                unique_deals.append(deal)
        
        logger.info(f"중복 제거 후 총 {len(unique_deals)}개 상품 (원래: {len(all_deals)}개)")
        
        # 수집된 상품 정보 저장
        save_deals_to_csv(unique_deals)
    else:
        logger.error("수집된 상품이 없습니다. 샘플 데이터 생성을 시작합니다.")
        
        # 샘플 데이터 생성 및 저장
        sample_deals = create_sample_deals()
        save_deals_to_csv(sample_deals, test_mode=True)
    
    logger.info("=== 쿠팡 핫딜 크롤링 종료 ===")

if __name__ == "__main__":
    main()
