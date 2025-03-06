# 쿠팡 핫딜 크롤러 (Selenium 버전)
import pandas as pd
import datetime
import time
import random
import os
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

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

# 크롤링 설정
MAX_PAGES = 3
SCROLL_PAUSE_TIME = 1.5
MAX_SCROLL_COUNT = 10

def setup_driver(headless=True):
    """Selenium 웹드라이버 설정"""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    
    # GitHub Actions에서 실행 시 필요한 옵션들
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # User-Agent 설정
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # 자동으로 최신 ChromeDriver 설치 및 사용
    driver_path = ChromeDriverManager().install()
    logger.info(f"ChromeDriver 경로: {driver_path}")
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    return driver

def scroll_to_bottom(driver, max_scrolls=MAX_SCROLL_COUNT):
    """페이지를 아래로 스크롤하여 모든 상품 로드"""
    logger.info("페이지 스크롤 시작...")
    
    # 초기 높이
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    scroll_count = 0
    while scroll_count < max_scrolls:
        # 페이지 끝까지 스크롤
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # 페이지 로딩 대기
        time.sleep(SCROLL_PAUSE_TIME)
        
        # 새 스크롤 높이 계산
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # 스크롤이 더 이상 내려가지 않으면 종료
        if new_height == last_height:
            logger.info(f"더 이상 스크롤할 내용이 없습니다. (스크롤 횟수: {scroll_count+1})")
            break
            
        last_height = new_height
        scroll_count += 1
        logger.info(f"스크롤 진행 중... ({scroll_count}/{max_scrolls})")
    
    logger.info("페이지 스크롤 완료")

def get_coupang_deals(page=1):
    """쿠팡 로켓 와우 핫딜 페이지에서 상품 정보 수집 (Selenium 사용)"""
    
    # 쿠팡 와우 핫딜 페이지 주소
    url = f"https://www.coupang.com/np/campaigns/82/components/194176?page={page}"
    logger.info(f"URL 요청: {url}")
    
    driver = setup_driver(headless=True)
    
    try:
        # 페이지 로드
        driver.get(url)
        logger.info("페이지 로드 완료")
        
        # 페이지 로딩 대기
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.baby-product-list, ul.productList"))
            )
            logger.info("상품 목록 로드 완료")
        except TimeoutException:
            logger.warning("상품 목록 로드 시간 초과")
        
        # 페이지 스크롤하여 모든 상품 로드
        scroll_to_bottom(driver)
        
        # 페이지 소스 가져오기
        page_source = driver.page_source
        
        # HTML 저장 (디버깅용)
        os.makedirs("debug", exist_ok=True)
        with open(f"debug/coupang_page_{page}.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(page_source, "html.parser")
        
        # 다양한 상품 목록 셀렉터 시도
        item_selectors = [
            "ul.baby-product-list li.baby-product",
            "ul.productList li.baby-product",
            "li.search-product",
            "ul.products li.product-item"
        ]
        
        items = []
        for selector in item_selectors:
            items = soup.select(selector)
            if items:
                logger.info(f"셀렉터 '{selector}'로 {len(items)}개 상품 찾음")
                break
        
        if not items:
            # Selenium으로 직접 요소 찾기 시도
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, "li.baby-product")
                if elements:
                    logger.info(f"Selenium으로 {len(elements)}개 상품 찾음")
                    # 여기서 직접 처리하거나 다시 BeautifulSoup으로 파싱할 수 있음
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")
                    items = soup.select("li.baby-product")
            except NoSuchElementException:
                logger.warning("Selenium으로도 상품을 찾을 수 없음")
        
        if not items:
            logger.warning("상품 목록을 찾을 수 없음")
            return []
        
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
        
        return deals
        
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}")
        return []
    
    finally:
        # 브라우저 종료
        driver.quit()
        logger.info("브라우저 종료")

def get_goldbox_deals():
    """쿠팡 골드박스 핫딜 페이지에서 상품 정보 수집 (Selenium 사용)"""
    
    url = "https://www.coupang.com/np/goldbox"
    logger.info(f"URL 요청: {url}")
    
    driver = setup_driver(headless=True)
    
    try:
        # 페이지 로드
        driver.get(url)
        logger.info("골드박스 페이지 로드 완료")
        
        # 페이지 로딩 대기
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.goldbox-list, div.product-item"))
            )
            logger.info("골드박스 상품 목록 로드 완료")
        except TimeoutException:
            logger.warning("골드박스 상품 목록 로드 시간 초과")
        
        # 페이지 스크롤하여 모든 상품 로드
        scroll_to_bottom(driver)
        
        # 페이지 소스 가져오기
        page_source = driver.page_source
        
        # HTML 저장 (디버깅용)
        os.makedirs("debug", exist_ok=True)
        with open("debug/coupang_goldbox.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(page_source, "html.parser")
        
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
            # Selenium으로 직접 요소 찾기 시도
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, "li.product-item")
                if elements:
                    logger.info(f"Selenium으로 {len(elements)}개 상품 찾음")
                    page_source = driver.page_source
                    soup = BeautifulSoup(page_source, "html.parser")
                    items = soup.select("li.product-item")
            except NoSuchElementException:
                logger.warning("Selenium으로도 상품을 찾을 수 없음")
        
        if not items:
            logger.warning("상품 목록을 찾을 수 없음")
            return []
        
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
        
        return deals
        
    except Exception as e:
        logger.error(f"골드박스 크롤링 중 오류 발생: {e}")
        return []
    
    finally:
        # 브라우저 종료
        driver.quit()
        logger.info("브라우저 종료")

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
            delay = random.uniform(2, 4)
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
        logger.error("수집된 상품이 없습니다.")
    
    logger.info("=== 쿠팡 핫딜 크롤링 종료 ===")

if __name__ == "__main__":
    main()
