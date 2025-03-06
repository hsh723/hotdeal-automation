# 쿠팡 핫딜 크롤러
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import random
import os

# 가상의 브라우저처럼 보이게 설정
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

def get_coupang_deals(page=1, retries=3):
    """쿠팡 로켓 와우 핫딜 페이지에서 상품 정보 수집"""
    
    # 쿠팡 와우 핫딜 페이지 주소
    url = f"https://www.coupang.com/np/campaigns/82/components/194176?page={page}"
    print(f"URL 요청: {url}")
    
    for attempt in range(retries):
        try:
            # 페이지 가져오기
            response = requests.get(url, headers=headers, timeout=10)
            
            # 응답 확인
            print(f"응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                # HTML 파싱
                soup = BeautifulSoup(response.text, "html.parser")
                
                # 상품 목록 찾기
                items = soup.select("ul.productList li.baby-product")
                print(f"찾은 상품 수: {len(items)}")
                
                # 첫 번째 상품 HTML 구조 확인 (디버깅)
                if items:
                    print("첫 번째 상품 HTML 구조:")
                    print(str(items[0])[:500] + "...")  # 앞부분만 출력
                
                deals = []
                for item in items:
                    # 상품 정보 추출
                    try:
                        title_elem = item.select_one("div.name")
                        if not title_elem:
                            print("제목 요소를 찾을 수 없음")
                            continue
                            
                        title = title_elem.text.strip()
                        
                        # 가격 정보
                        price_elem = item.select_one("strong.price-value")
                        if not price_elem:
                            print(f"가격 요소를 찾을 수 없음: {title}")
                            continue
                            
                        price = int(price_elem.text.replace(",", "")) if price_elem else 0
                        
                        # 원래 가격
                        original_price_elem = item.select_one("del.base-price")
                        original_price = int(original_price_elem.text.replace(",", "").replace("원", "")) if original_price_elem else price
                        
                        # 할인율 계산
                        discount = round((original_price - price) / original_price * 100) if original_price > price else 0
                        
                        # 링크 추출
                        link_elem = item.select_one("a.baby-product-link")
                        link = "https://www.coupang.com" + link_elem["href"] if link_elem else ""
                        
                        # 이미지 URL
                        img_elem = item.select_one("img.product-image")
                        image_url = img_elem["src"] if img_elem and "src" in img_elem.attrs else ""
                        
                        # 카테고리 (있는 경우)
                        category_elem = item.select_one("div.category")
                        category = category_elem.text.strip() if category_elem else "일반"
                        
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
                        
                        # 첫 번째 상품 정보 출력 (디버깅)
                        if len(deals) == 1:
                            print(f"첫 번째 상품 정보: {deals[0]}")
                            
                    except Exception as e:
                        print(f"상품 정보 추출 중 오류: {e}")
                
                return deals
            
            else:
                print(f"페이지 가져오기 실패: 상태 코드 {response.status_code}")
                # 응답 내용의 일부를 출력하여 확인
                print(f"응답 내용(앞부분): {response.text[:500]}")
        
        except requests.exceptions.RequestException as e:
            print(f"요청 오류 (재시도 {attempt+1}/{retries}): {e}")
            time.sleep(random.uniform(2, 5))  # 재시도 전 대기
    
    return []  # 모든 재시도 실패 시 빈 목록 반환

def main():
    """메인 함수: 여러 페이지의 핫딜 정보 수집"""
    
    all_deals = []
    
    # 처음 3페이지만 수집 (테스트용)
    for page in range(1, 4):
        print(f"페이지 {page} 수집 중...")
        
        # 핫딜 정보 가져오기
        deals = get_coupang_deals(page=page)
        all_deals.extend(deals)
        
        # IP 차단 방지를 위한 대기
        time.sleep(random.uniform(3, 5))
    
   if all_deals:
        # DataFrame으로 변환
        df = pd.DataFrame(all_deals)
        
        # 데이터 저장 폴더 생성
        os.makedirs("data", exist_ok=True)
        
        # 오늘 날짜로 파일명 생성
        today = datetime.datetime.now().strftime("%Y%m%d")
        file_path = f"data/coupang_deals_{today}.csv"
        
        # CSV 파일로 저장
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"수집 완료: {len(all_deals)}개 상품, 저장 경로: {file_path}")
    else:
        print("수집된 상품이 없습니다.")
        
        # 테스트용 샘플 데이터 생성 (실제 크롤링 실패 시 테스트용)
        sample_deals = [
            {
                "title": "샘플 상품 1 - 크롤링 테스트용",
                "price": 10000,
                "original_price": 20000,
                "discount": 50,
                "link": "https://www.coupang.com/sample1",
                "image_url": "https://www.coupang.com/sample1.jpg",
                "category": "테스트",
                "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            {
                "title": "샘플 상품 2 - 크롤링 테스트용",
                "price": 15000,
                "original_price": 25000,
                "discount": 40,
                "link": "https://www.coupang.com/sample2",
                "image_url": "https://www.coupang.com/sample2.jpg",
                "category": "테스트",
                "crawled_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        ]
        df = pd.DataFrame(sample_deals)
        os.makedirs("data", exist_ok=True)
        today = datetime.datetime.now().strftime("%Y%m%d")
        file_path = f"data/coupang_deals_{today}.csv"
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"테스트용 샘플 데이터 생성 완료: {len(sample_deals)}개 상품, 저장 경로: {file_path}")
    else:
        print("수집된 상품이 없습니다.")

if __name__ == "__main__":
    main()
