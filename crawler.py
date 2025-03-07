# 쿠팡 핫딜 크롤러 (Selenium 버전)
import pandas as pd
import datetime
import time
import random
import os
import logging
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
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

# 환경 변수에서 설정 가져오기
MAX_PAGES = int(os.environ.get('MAX_PAGES', '3'))
SCROLL_PAUSE_TIME = float(os.environ.get('SCROLL_PAUSE_TIME', '1.5'))
MAX_SCROLL_COUNT = int(os.environ.get('MAX_SCROLL_COUNT', '10'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
DEBUG_MODE = os.environ.get('DEBUG_MODE', '').lower() == 'true'
HEADLESS_MODE = os.environ.get('HEADLESS', '').lower() != 'false'  # 기본값은 headless 모드

def setup_driver(headless=HEADLESS_MODE):
    """Selenium 웹드라이버 설정"""
    for attempt in range(MAX_RETRIES):
        try:
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
            
            # 환경 변수로 시스템 ChromeDriver 사용 여부 확인
            use_system_driver = os.environ.get('USE_SYSTEM_CHROMEDRIVER', '').lower() == 'true'
            
            if use_system_driver:
                # 시스템에 설치된 ChromeDriver 사용
                driver_path = "/usr/bin/chromedriver"
                logger.info(f"시스템 ChromeDriver 사용: {driver_path}")
            else:
                # 자동으로 최신 ChromeDriver 설치 및 사용
                driver_path = ChromeDriverManager().install()
                logger.info(f"WebDriverManager ChromeDriver 사용: {driver_path}")
            
            service = Service(executable_path=driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 페이지 로드 타임아웃 설정
            driver.set_page_load_timeout(30)
            
            return driver
        
        except WebDriverException as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"WebDriver 초기화 실패 ({attempt+1}/{MAX_RETRIES}): {e}")
                time.sleep(2)
            else:
                logger.error(f"WebDriver 초기화 최종 실패: {e}")
                raise
        except Exception as e:
            logger.error(f"WebDriver 초기화 중 예상치 못한 오류: {e}")
            raise

def scroll_to_bottom(driver, max_scrolls=MAX_SCROLL_COUNT):
    """페이지를 아래로 스크롤하여 모든 상품 로드"""
    logger.info("페이지 스크롤 시작...")
    
    # 초기 높이
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    scroll_count = 0
    while scroll_count < max_scrolls:
        try:
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
        
        except Exception as e:
            logger.warning(f"스크롤 중 오류 발생: {e}")
            break
    
    logger.info("페이지 스크롤 완료")

def get_page_with_retry(driver, url, max_retries=MAX_RETRIES):
    """재시도 로직이 포함된 페이지 로드 함수"""
    for attempt in range(max_retries):
        try:
            driver.get(url)
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"페이지 로드 실패 ({attempt+1}/{max_retries}): {e}")
                time.sleep(2 * (attempt + 1))  # 지수 백오프
            else:
                logger.error(f"페이지 로드 최종 실패: {e}")
                return False

def get_coupang_deals(page=1):
    """쿠팡 로켓 와우 핫딜 페이지에서 상품 정보 수집 (Selenium 사용)"""
    
    # 쿠팡 와우 핫딜 페이지 주소
    url = f"https://www.coupang.com/np/campaigns/82/components/194176?page={page}"
    logger.info(f"URL 요청: {url}")
    
    driver = None
    try:
        driver = setup_driver(headless=HEADLESS_MODE)
        
        # 페이지 로드 (재시도 로직 포함)
        if not get_page_with_retry(driver, url):
            logger.error(f"페이지 {page} 로드 실패")
            return []
        
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
        
        # HTML 저장 (디버깅용, DEBUG_MODE가 True일 때만)
        if DEBUG_MODE:
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
            "ul.products li.product-item",
            "div.product-item"  # 추가 셀렉터
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
                selectors_to_try = ["li.baby-product", "div.product-item", "li.product-item"]
                for selector in selectors_to_try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"Selenium으로 '{selector}' 셀렉터를 사용해 {len(elements)}개 상품 찾음")
                        page_source = driver.page_source
                        soup = BeautifulSoup(page_source, "html.parser")
                        items = soup.select(selector)
                        break
            except NoSuchElementException:
                logger.warning("Selenium으로도 상품을 찾을 수 없음")
        
        if not items:
            logger.warning("상품 목록을 찾을 수 없음")
            return []
        
        deals = []
        for item in items:
            # 상품 정보 추출
            try:
                # 기본값 설정
                product_info = {
                    "title": "",
                    "price": 0,
                    "original_price": 0,
                    "discount": 0,
                    "link": "",
                    "image_url": "",
                    "category": "일반",
                    "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # 다양한 제목 셀렉터 시도
                title_selectors = ["div.name", "div.product-name", "div.title"]
                for selector in title_selectors:
                    title_elem = item.select_one(selector)
                    if title_elem:
                        product_info["title"] = title_elem.text.strip()
                        break
                
                if not product_info["title"]:
                    logger.warning("제목 요소를 찾을 수 없음")
                    continue
                
                # 다양한 가격 셀렉터 시도
                price_selectors = ["strong.price-value", "strong.price", "em.sale", "div.price"]
                for selector in price_selectors:
                    price_elem = item.select_one(selector)
                    if price_elem:
                        price_text = price_elem.text.strip()
                        price_text = ''.join(filter(str.isdigit, price_text))
                        if price_text:
                            product_info["price"] = int(price_text)
                            break
                
                if product_info["price"] == 0:
                    logger.warning(f"가격 요소를 찾을 수 없음: {product_info['title']}")
                    continue
                
                # 다양한 원래 가격 셀렉터 시도
                original_price_selectors = ["del.base-price", "span.origin-price", "del.original-price"]
                for selector in original_price_selectors:
                    original_price_elem = item.select_one(selector)
                    if original_price_elem:
                        original_price_text = original_price_elem.text.strip()
                        original_price_text = ''.join(filter(str.isdigit, original_price_text))
                        if original_price_text:
                            product_info["original_price"] = int(original_price_text)
                            break
                
                # 원래 가격이 없으면 현재 가격으로 설정
                if product_info["original_price"] == 0:
                    product_info["original_price"] = product_info["price"]
                
                # 할인율 계산 또는 추출
                discount_selectors = ["span.discount-rate", "span.discount-percentage", "div.discount-rate"]
                for selector in discount_selectors:
                    discount_elem = item.select_one(selector)
                    if discount_elem:
                        discount_text = discount_elem.text.strip()
                        discount_text = ''.join(filter(str.isdigit, discount_text))
                        if discount_text:
                            product_info["discount"] = int(discount_text)
                            break
                
                # 할인율이 없으면 계산
                if product_info["discount"] == 0 and product_info["original_price"] > product_info["price"]:
                    product_info["discount"] = round((product_info["original_price"] - product_info["price"]) / product_info["original_price"] * 100)
                
                # 링크 추출
                link_selectors = ["a.baby-product-link", "a.product-link", "a"]
                for selector in link_selectors:
                    link_elems = item.select(selector)
                    for link_elem in link_elems:
                        if link_elem.has_attr("href") and ("/vp/" in link_elem["href"] or "/np/" in link_elem["href"]):
                            product_info["link"] = "https://www.coupang.com" + link_elem["href"]
                            break
                    if product_info["link"]:
                        break
                
                # 이미지 URL
                img_selectors = ["img.product-image", "img.search-product-wrap-img", "img"]
                for selector in img_selectors:
                    img_elem = item.select_one(selector)
                    if img_elem and img_elem.has_attr("src"):
                        image_url = img_elem["src"]
                        if not image_url.startswith("http"):
                            image_url = "https:" + image_url
                        product_info["image_url"] = image_url
                        break
                
                # 카테고리 (있는 경우)
                category_selectors = ["div.category", "span.category"]
                for selector in category_selectors:
                    category_elem = item.select_one(selector)
                    if category_elem:
                        product_info["category"] = category_elem.text.strip()
                        break
                
                # 필수 필드가 있는지 확인
                if product_info["title"] and product_info["price"] > 0 and product_info["link"]:
                    deals.append(product_info)
                
            except Exception as e:
                logger.error(f"상품 정보 추출 중 오류: {e}")
                continue  # 한 상품 처리 실패해도 계속 진행
        
        return deals
        
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}")
        return []
    
    finally:
        # 브라우저 종료
        if driver:
            try:
                driver.quit()
                logger.info("브라우저 종료")
            except Exception as e:
                logger.warning(f"브라우저 종료 중 오류: {e}")

def get_goldbox_deals():
    """쿠팡 골드박스 핫딜 페이지에서 상품 정보 수집 (Selenium 사용)"""
    
    # 여러 URL 패턴 시도
    urls = [
        "https://www.coupang.com/np/goldbox",
        "https://www.coupang.com/np/campaigns/82"  # 대체 URL
    ]
    
    for url_index, url in enumerate(urls):
        logger.info(f"URL 요청 ({url_index+1}/{len(urls)}): {url}")
        
        driver = None
        try:
            driver = setup_driver(headless=HEADLESS_MODE)
            
            # 페이지 로드 (재시도 로직 포함)
            if not get_page_with_retry(driver, url):
                logger.error(f"골드박스 URL {url} 로드 실패, 다음 URL 시도")
                continue
            
            logger.info("골드박스 페이지 로드 완료")
            
            # 페이지 로딩 대기
            try:
                selectors_to_wait = [
                    "ul.goldbox-list", 
                    "div.product-item", 
                    "ul.baby-product-list"
                ]
                
                for selector in selectors_to_wait:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        logger.info(f"골드박스 상품 목록 로드 완료 (셀렉터: {selector})")
                        break
                    except TimeoutException:
                        continue
            except TimeoutException:
                logger.warning("골드박스 상품 목록 로드 시간 초과")
            
            # 페이지 스크롤하여 모든 상품 로드
            scroll_to_bottom(driver)
            
            # 페이지 소스 가져오기
            page_source = driver.page_source
            
            # HTML 저장 (디버깅용, DEBUG_MODE가 True일 때만)
            if DEBUG_MODE:
                os.makedirs("debug", exist_ok=True)
                with open(f"debug/coupang_goldbox_{url_index}.html", "w", encoding="utf-8") as f:
                    f.write(page_source)
            
            # BeautifulSoup으로 파싱
            soup = BeautifulSoup(page_source, "html.parser")
            
            # 상품 목록 찾기
            item_selectors = [
                "ul.goldbox-list li.product-item",
                "div.product-item",
                "ul.productList li.product-item",
                "ul.baby-product-list li.baby-product"  # 추가 셀렉터
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
                    selectors_to_try = ["li.product-item", "div.product-item", "li.baby-product"]
                    for selector in selectors_to_try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            logger.info(f"Selenium으로 '{selector}' 셀렉터를 사용해 {len(elements)}개 상품 찾음")
                            page_source = driver.page_source
                            soup = BeautifulSoup(page_source, "html.parser")
                            items = soup.select(selector)
                            break
                except NoSuchElementException:
                    logger.warning("Selenium으로도 상품을 찾을 수 없음")
            
            if not items:
                logger.warning(f"URL {url}에서 상품 목록을 찾을 수 없음, 다음 URL 시도")
                continue
            
            deals = []
            for item in items:
                # 상품 정보 추출
                try:
                    # 기본값 설정
                    product_info = {
                        "title": "",
                        "price": 0,
                        "original_price": 0,
                        "discount": 0,
                        "link": "",
                        "image_url": "",
                        "category": "골드박스",
                        "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # 상품명
                    title_selectors = ["div.name", "div.title", "div.product-name"]
                    for selector in title_selectors:
                        title_elem = item.select_one(selector)
                        if title_elem:
                            product_info["title"] = title_elem.text.strip()
                            break
                    
                    if not product_info["title"]:
                        logger.warning("제목 요소를 찾을 수 없음")
                        continue
                    
                    # 현재 가격
                    price_selectors = ["strong.price-value", "strong.price", "div.price"]
                    for selector in price_selectors:
                        price_elem = item.select_one(selector)
                        if price_elem:
                            price_text = price_elem.text.strip()
                            price_text = ''.join(filter(str.isdigit, price_text))
                            if price_text:
                                product_info["price"] = int(price_text)
                                break
                    
                    if product_info["price"] == 0:
                        logger.warning(f"가격 요소를 찾을 수 없음: {product_info['title']}")
                        continue
                    
                    # 원래 가격
                    original_price_selectors = ["span.base-price", "del.base-price", "del.original-price"]
                    for selector in original_price_selectors:
                        original_price_elem = item.select_one(selector)
                        if original_price_elem:
                            original_price_text = original_price_elem.text.strip()
                            original_price_text = ''.join(filter(str.isdigit, original_price_text))
                            if original_price_text:
                                product_info["original_price"] = int(original_price_text)
                                break
                    
                    # 원래 가격이 없으면 현재 가격으로 설정
                    if product_info["original_price"] == 0:
                        product_info["original_price"] = product_info["price"]
                    
                    # 할인율
                    discount_selectors = ["span.discount-percentage", "span.discount-rate", "div.discount-rate"]
                    for selector in discount_selectors:
                        discount_elem = item.select_one(selector)
                        if discount_elem:
                            discount_text = discount_elem.text.strip()
                            discount_text = ''.join(filter(str.isdigit, discount_text))
                            if discount_text:
                                product_info["discount"] = int(discount_text)
                                break
                    
                    # 할인율이 없으면 계산
                    if product_info["discount"] == 0 and product_info["original_price"] > product_info["price"]:
                        product_info["discount"] = round((product_info["original_price"] - product_info["price"]) / product_info["original_price"] * 100)
                    
                    # 링크
                    link_selectors = ["a.product-link", "a"]
                    for selector in link_selectors:
                        link_elems = item.select(selector)
                        for elem in link_elems:
                            if elem.has_attr("href") and ("/vp/" in elem["href"] or "/np/" in elem["href"]):
                                product_info["link"] = "https://www.coupang.com" + elem["href"]
                                break
                        if product_info["link"]:
                            break
                    
                    # 이미지 URL
                    img_selectors = ["img.product-image", "img"]
                    for selector in img_selectors:
                        img_elem = item.select_one(selector)
                        if img_elem and img_elem.has_attr("src"):
                            image_url = img_elem["src"]
                            if not image_url.startswith("http"):
                                image_url = "https:" + image_url
                            product_info["image_url"] = image_url
                            break
                    
                    # 필수 필드가 있는지 확인
                    if product_info["title"] and product_info["price"] > 0 and product_info["link"]:
                        deals.append(product_info)
                    
                except Exception as e:
                    logger.error(f"상품 정보 추출 중 오류: {e}")
                    continue  # 한 상품 처리 실패해도 계속 진행
            
            # 상품을 찾았으면 반환
            if deals:
                logger.info(f"골드박스에서 {len(deals)}개 상품 수집 완료")
                return deals
            else:
                logger.warning(f"URL {url}에서 상품 정보를 추출할 수 없음, 다음 URL 시도")
            
        except Exception as e:
            logger.error(f"골드박스 크롤링 중 오류 발생 (URL: {url}): {e}")
            continue  # 다음 URL 시도
        
        finally:
            # 브라우저 종료
            if driver:
                try:
                    driver.quit()
                    logger.info("브라우저 종료")
                except Exception as e:
                    logger.warning(f"브라우저 종료 중 오류: {e}")
    
    # 모든 URL 시도 후에도 실패한 경우
    logger.error("모든 골드박스 URL에서 크롤링 실패")
    return []

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
        logger.warning("골드박스 핫딜 수집 실패, 일반 핫딜 수집으로 진행")
    
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
