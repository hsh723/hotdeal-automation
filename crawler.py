# 쿠팡 핫딜 크롤러 (Selenium 버전)
import os
import pandas as pd
import datetime
import time
import random
import logging
import json
from dotenv import load_dotenv
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

# .env 파일에서 환경변수 로드
load_dotenv()

# 크롤링 설정
MAX_PAGES = int(os.getenv("MAX_PAGES", "3"))
DELAY_MIN = float(os.getenv("DELAY_MIN", "3"))
DELAY_MAX = float(os.getenv("DELAY_MAX", "5"))
HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"

class SeleniumCoupangCrawler:
    def __init__(self, headless=HEADLESS):
        self.setup_driver(headless)
        
    def setup_driver(self, headless=True):
        """Selenium WebDriver 설정"""
        try:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless=new")
            
            # 기본 옵션 추가
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # 봇 탐지 방지 옵션
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)
            
            # 랜덤 User-Agent 설정
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # 자동 설치되는 WebDriver 사용
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # 쿠키 및 캐시 초기화
            self.driver.delete_all_cookies()
            
            # JavaScript 실행 우회 코드
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            })
            
            logger.info("Selenium WebDriver 초기화 완료")
            
        except Exception as e:
            logger.error(f"Driver 초기화 중 오류: {e}", exc_info=True)
            raise
    
    def close(self):
        """드라이버 종료"""
        if hasattr(self, 'driver'):
            try:
                self.driver.quit()
                logger.info("WebDriver 종료됨")
            except Exception as e:
                logger.error(f"WebDriver 종료 중 오류: {e}")
    
    def get_page(self, url, max_retries=3):
        """페이지 로드 및 렌더링 대기"""
        for attempt in range(max_retries):
            try:
                logger.info(f"URL 요청: {url} (시도 {attempt+1}/{max_retries})")
                self.driver.get(url)
                
                # 페이지 로딩 대기
                time.sleep(random.uniform(2, 4))
                
                # 스크롤 다운 (동적 로딩 콘텐츠를 위해)
                self.scroll_down()
                
                # 페이지 소스 일부 확인 (디버깅)
                page_source = self.driver.page_source[:1000]
                logger.info(f"페이지 소스 일부: {page_source[:200]}...")
                
                # 디버깅용 HTML 저장
                debug_dir = "debug"
                os.makedirs(debug_dir, exist_ok=True)
                with open(f"{debug_dir}/page_{int(time.time())}.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)
                
                # 스크린샷 저장 (디버깅)
                self.driver.save_screenshot(f"{debug_dir}/screenshot_{int(time.time())}.png")
                
                return True
                
            except WebDriverException as e:
                logger.error(f"페이지 로드 중 오류 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 지수 백오프
                    logger.info(f"{wait_time}초 후 재시도...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"최대 재시도 횟수 초과: {url}")
                    return False
    
    def scroll_down(self, scroll_pause_time=1):
        """페이지를 아래로 스크롤하여 동적 콘텐츠 로드"""
        try:
            # 페이지 높이 가져오기
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # 스크롤 다운
            for _ in range(3):  # 3번 스크롤
                # 페이지의 1/3씩 스크롤
                self.driver.execute_script(f"window.scrollTo(0, {last_height/3});")
                time.sleep(scroll_pause_time/2)
                self.driver.execute_script(f"window.scrollTo(0, {2*last_height/3});")
                time.sleep(scroll_pause_time/2)
                self.driver.execute_script(f"window.scrollTo(0, {last_height});")
                time.sleep(scroll_pause_time)
                
                # 새 높이 계산
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                
            logger.info("페이지 스크롤 완료")
            
        except Exception as e:
            logger.error(f"스크롤 중 오류: {e}")
    
    def extract_product_info(self, selectors):
        """다양한 셀렉터를 시도하여 제품 정보 추출"""
        products = []
        
        for item_selector in selectors["item_selectors"]:
            try:
                items = self.driver.find_elements(By.CSS_SELECTOR, item_selector)
                if items:
                    logger.info(f"셀렉터 '{item_selector}'로 {len(items)}개 상품 찾음")
                    
                    for item in items:
                        try:
                            # 상품명 추출
                            title = None
                            for title_selector in selectors["title_selectors"]:
                                try:
                                    title_elem = item.find_element(By.CSS_SELECTOR, title_selector)
                                    title = title_elem.text.strip()
                                    if title:
                                        break
                                except NoSuchElementException:
                                    continue
                            
                            if not title:
                                logger.warning("상품명을 찾을 수 없음")
                                continue
                            
                            # 현재 가격 추출
                            price = None
                            for price_selector in selectors["price_selectors"]:
                                try:
                                    price_elem = item.find_element(By.CSS_SELECTOR, price_selector)
                                    price_text = price_elem.text.strip()
                                    price_text = ''.join(filter(str.isdigit, price_text))
                                    if price_text:
                                        price = int(price_text)
                                        break
                                except (NoSuchElementException, ValueError):
                                    continue
                            
                            if not price:
                                logger.warning(f"가격을 찾을 수 없음: {title}")
                                continue
                            
                            # 원래 가격 추출
                            original_price = price  # 기본값
                            for original_price_selector in selectors["original_price_selectors"]:
                                try:
                                    original_price_elem = item.find_element(By.CSS_SELECTOR, original_price_selector)
                                    original_price_text = original_price_elem.text.strip()
                                    original_price_text = ''.join(filter(str.isdigit, original_price_text))
                                    if original_price_text:
                                        original_price = int(original_price_text)
                                        break
                                except (NoSuchElementException, ValueError):
                                    continue
                            
                            # 할인율 추출 또는 계산
                            discount = 0
                            for discount_selector in selectors["discount_selectors"]:
                                try:
                                    discount_elem = item.find_element(By.CSS_SELECTOR, discount_selector)
                                    discount_text = discount_elem.text.strip()
                                    discount_text = ''.join(filter(str.isdigit, discount_text))
                                    if discount_text:
                                        discount = int(discount_text)
                                        break
                                except (NoSuchElementException, ValueError):
                                    continue
                            
                            # 할인율이 없으면 계산
                            if discount == 0 and original_price > price:
                                discount = round((original_price - price) / original_price * 100)
                            
                            # 링크 추출
                            link = ""
                            for link_selector in selectors["link_selectors"]:
                                try:
                                    link_elem = item.find_element(By.CSS_SELECTOR, link_selector)
                                    link = link_elem.get_attribute("href")
                                    if link:
                                        break
                                except NoSuchElementException:
                                    continue
                            
                            # 이미지 URL 추출
                            image_url = ""
                            for image_selector in selectors["image_selectors"]:
                                try:
                                    img_elem = item.find_element(By.CSS_SELECTOR, image_selector)
                                    image_url = img_elem.get_attribute("src")
                                    if image_url:
                                        break
                                except NoSuchElementException:
                                    continue
                            
                            # 제품 정보 저장
                            product = {
                                "title": title,
                                "price": price,
                                "original_price": original_price,
                                "discount": discount,
                                "link": link,
                                "image_url": image_url,
                                "category": "핫딜",
                                "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            
                            products.append(product)
                            logger.info(f"상품 정보 추출: {title[:30]}...")
                            
                        except Exception as e:
                            logger.error(f"상품 정보 추출 중 오류: {e}")
                    
                    # 상품을 찾았으면 반복 중단
                    if products:
                        break
            except Exception as e:
                logger.error(f"상품 목록 검색 중 오류: {e}")
        
        return products
    
    def get_dealsite_selectors(self):
        """쿠팡 핫딜 페이지 셀렉터"""
        return {
            "item_selectors": [
                "ul.baby-product-list > li.baby-product",
                "ul.productList > li.product-item",
                "li.search-product",
                "div.baby-product",
                "div.product-item"
            ],
            "title_selectors": [
                "div.name",
                "div.title",
                "div.product-name",
                "a.product-link > div.title"
            ],
            "price_selectors": [
                "strong.price-value", 
                "div.price > em.sale",
                "div.price-area > div.price-wrap > div.price > em",
                "div.price-area > strong"
            ],
            "original_price_selectors": [
                "del.base-price",
                "span.price-info > span.origin-price",
                "span.original-price"
            ],
            "discount_selectors": [
                "span.discount-percentage",
                "div.discount-rate",
                "span.discount-rate",
                "div.discount-percent"
            ],
            "link_selectors": [
                "a.baby-product-link",
                "a.product-link",
                "a.search-product-link"
            ],
            "image_selectors": [
                "img.product-image",
                "img.search-product-wrap-img",
                "img.product-img"
            ]
        }
    
    def get_goldbox_selectors(self):
        """쿠팡 골드박스 페이지 셀렉터"""
        return {
            "item_selectors": [
                "ul.goldbox-list > li.product-item",
                "div.product-item",
                "li.product-item"
            ],
            "title_selectors": [
                "div.name",
                "div.title"
            ],
            "price_selectors": [
                "strong.price-value",
                "div.price"
            ],
            "original_price_selectors": [
                "span.base-price",
                "del.base-price"
            ],
            "discount_selectors": [
                "span.discount-percentage",
                "span.discount-rate"
            ],
            "link_selectors": [
                "a.product-link",
                "a"
            ],
            "image_selectors": [
                "img.product-image",
                "img"
            ]
        }
    
    def crawl_goldbox(self):
        """쿠팡 골드박스 페이지 크롤링"""
        url = "https://www.coupang.com/np/goldbox"
        logger.info("골드박스 핫딜 크롤링 시작")
        
        if self.get_page(url):
            selectors = self.get_goldbox_selectors()
            products = self.extract_product_info(selectors)
            
            if products:
                logger.info(f"골드박스 핫딜 {len(products)}개 찾음")
                # 골드박스 카테고리 표시
                for product in products:
                    product["category"] = "골드박스"
                return products
            else:
                logger.warning("골드박스 핫딜 상품을 찾을 수 없음")
        
        return []
    
    def crawl_deal_page(self, page=1):
        """쿠팡 핫딜 페이지 크롤링"""
        urls = [
            f"https://www.coupang.com/np/campaigns/82?page={page}",
            f"https://www.coupang.com/np/campaigns/82/components/194176?page={page}",
            f"https://www.coupang.com/np/campaigns/82?componentId=194176&page={page}"
        ]
        
        logger.info(f"핫딜 페이지 {page} 크롤링 시작")
        
        for url in urls:
            if self.get_page(url):
                selectors = self.get_dealsite_selectors()
                products = self.extract_product_info(selectors)
                
                if products:
                    logger.info(f"핫딜 페이지 {page}에서 {len(products)}개 상품 찾음")
                    return products
            
            # 다음 URL 시도 전 대기
            time.sleep(random.uniform(1, 2))
        
        logger.warning(f"페이지 {page}에서 상품을 찾을 수 없음")
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
    """메인 크롤링 함수"""
    logger.info("=== 쿠팡 핫딜 크롤링 시작 (Selenium) ===")
    
    # 모든 수집 결과를 저장할 리스트
    all_deals = []
    
    try:
        # 크롤러 인스턴스 생성
        crawler = SeleniumCoupangCrawler(headless=HEADLESS)
        
        # 골드박스 크롤링
        goldbox_deals = crawler.crawl_goldbox()
        if goldbox_deals:
            all_deals.extend(goldbox_deals)
            logger.info(f"골드박스 핫딜 {len(goldbox_deals)}개 수집 완료")
        
        # 일반 핫딜 페이지 크롤링
        for page in range(1, MAX_PAGES + 1):
            page_deals = crawler.crawl_deal_page(page)
            if page_deals:
                all_deals.extend(page_deals)
                logger.info(f"페이지 {page}에서 {len(page_deals)}개 상품 수집 완료")
            
            # 다음 페이지 요청 전 대기
            if page < MAX_PAGES:
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"다음 페이지 요청 전 {delay:.2f}초 대기 중...")
                time.sleep(delay)
    
    except Exception as e:
        logger.error(f"크롤링 중 예외 발생: {e}", exc_info=True)
    
    finally:
        # 드라이버 종료
        if 'crawler' in locals():
            crawler.close()
    
    # 수집 결과 처리
    if all_deals:
        # 중복 제거 (URL 기준)
        unique_deals = []
        seen_links = set()
        
        for deal in all_deals:
            if deal["link"] not in seen_links:
                seen_links.add(deal["link"])
                unique_deals.append(deal)
        
        logger.info(f"중복 제거 후 총 {len(unique_deals)}개 상품 (원래: {len(all_deals)}개)")
        
        # CSV 파일로 저장
        save_deals_to_csv(unique_deals)
    else:
        logger.error("수집된 상품이 없습니다. 샘플 데이터 생성을 시작합니다.")
        
        # 샘플 데이터 생성 및 저장
        sample_deals = create_sample_deals()
        save_deals_to_csv(sample_deals, test_mode=True)
    
    logger.info("=== 쿠팡 핫딜 크롤링 종료 ===")

if __name__ == "__main__":
    main()
