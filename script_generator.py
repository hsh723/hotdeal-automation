#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import glob
import datetime
import pandas as pd
from collections import defaultdict

def get_latest_csv_file(directory="data"):
    """최신 핫딜 CSV 파일 경로 반환"""
    pattern = os.path.join(directory, "coupang_deals_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"'{pattern}' 패턴과 일치하는 파일을 찾을 수 없습니다.")
    return max(files, key=os.path.getctime)

def read_deals_from_csv(file_path):
    """CSV 파일에서 핫딜 상품 정보 읽기 (pandas DataFrame 반환)"""
    try:
        # pandas를 사용하여 CSV 파일 읽기
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # 필요한 열이 있는지 확인
        required_columns = ['title', 'price', 'original_price', 'discount', 'category', 'url']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None  # 없는 열은 None으로 초기화
        
        # 할인율 처리 - 문자열에서 숫자로 변환
        if 'discount' in df.columns:
            df['discount'] = df['discount'].apply(
                lambda x: int(str(x).strip('%')) if pd.notna(x) and str(x).strip('%').isdigit() else 0
            )
        
        # 제목 처리 - 빈 값이나 None 처리
        df['title'] = df['title'].apply(
            lambda x: x if pd.notna(x) and str(x).strip() != '' else '제목 없음'
        )
        
        print(f"CSV 파일에서 {len(df)}개의 상품 정보를 읽었습니다.")
        return df
    
    except Exception as e:
        print(f"CSV 파일 읽기 실패: {e}")
        # 오류 발생 시 빈 DataFrame 반환
        return pd.DataFrame(columns=['title', 'price', 'original_price', 'discount', 'category', 'url'])

def categorize_deals(deals_df, min_discount=20):
    """상품을 카테고리별로 분류하고 할인율 높은 순으로 정렬 (DataFrame 입력)"""
    categorized = defaultdict(list)
    
    # DataFrame 순회
    for _, deal in deals_df.iterrows():
        # 카테고리 처리
        category = deal.get('category', '기타')
        if pd.isna(category) or not category:
            category = '기타'
            
        # 할인율 확인
        discount = deal.get('discount', 0)
        if pd.isna(discount):
            discount = 0
        
        # 최소 할인율 이상인 상품만 추가
        if discount >= min_discount:
            # 상품 제목 처리
            title = deal.get('title', '제목 없음')
            if pd.isna(title) or not str(title).strip():
                title = '제목 없음'
            
            # 로그 추가
            print(f"상품 제목 확인: {title}, 카테고리: {category}, 할인율: {discount}%")
            
            categorized[category].append({
                'id': deal.get('id', ''),
                'title': title,
                'price': deal.get('price', '가격 정보 없음'),
                'original_price': deal.get('original_price', ''),
                'discount': discount,
                'url': deal.get('url', '')
            })
    
    # 각 카테고리 내에서 할인율 높은 순으로 정렬
    for category in categorized:
        categorized[category].sort(key=lambda x: x['discount'], reverse=True)
    
    return categorized

def generate_category_intro(category):
    """카테고리별 인트로 문구 생성"""
    intros = {
        "식품": "다음은 오늘의 식품 핫딜입니다. 맛있는 음식을 저렴하게 구매하세요!",
        "생활용품": "오늘의 생활용품 핫딜을 소개합니다. 일상을 더 편리하게 만들어줄 상품들입니다.",
        "전자제품": "놓치면 후회할 전자제품 핫딜입니다. 최신 기술을 저렴하게 만나보세요!",
        "패션": "스타일을 업그레이드할 패션 아이템 핫딜입니다. 트렌디한 아이템을 할인된 가격에 만나보세요!",
        "가구/인테리어": "집 분위기를 바꿔줄 가구와 인테리어 핫딜입니다. 공간을 더 아름답게 꾸며보세요!",
        "뷰티": "아름다움을 위한 뷰티 제품 핫딜입니다. 인기 화장품과 스킨케어 제품을 확인하세요!",
        "스포츠/레저": "건강한 생활을 위한 스포츠/레저 핫딜입니다. 활동적인 라이프스타일을 지원할 제품들입니다!",
        "디지털/가전": "최신 디지털 기기와 가전제품 핫딜입니다. 스마트한 생활을 도와줄 제품들을 만나보세요!",
        "도서": "지식과 재미를 선사할 도서 핫딜입니다. 베스트셀러와 신간을 저렴하게 구매하세요!",
        "완구/취미": "즐거운 시간을 위한 완구와 취미용품 핫딜입니다. 새로운 취미를 시작해보세요!"
    }
    
    return intros.get(category, f"다음은 {category} 카테고리의 핫딜 상품입니다. 놓치지 마세요!")

def generate_deal_script(deal):
    """개별 상품 설명 스크립트 생성"""
    script = f"'{deal['title']}'입니다. "
    
    if deal['price'] and deal['original_price']:
        script += f"정가 {deal['original_price']}원에서 {deal['discount']}% 할인된 {deal['price']}원에 구매 가능합니다. "
    elif deal['price']:
        script += f"가격은 {deal['price']}원이며, {deal['discount']}% 할인된 금액입니다. "
    
    return script

def generate_full_script(categorized_deals, max_items_per_category=5):
    """전체 스크립트 생성"""
    today = datetime.datetime.now().strftime("%Y년 %m월 %d일")
    script = f"안녕하세요! {today} 쿠팡 핫딜 정보를 소개해드립니다.\n\n"
    
    for category, deals in categorized_deals.items():
        if not deals:
            continue
            
        script += generate_category_intro(category) + "\n\n"
        
        for i, deal in enumerate(deals[:max_items_per_category], 1):
            script += f"{i}. {generate_deal_script(deal)}\n"
        
        script += "\n"
    
    script += "이상 오늘의 핫딜 정보였습니다. 구매하실 때는 가격 변동이 있을 수 있으니 최종 가격을 꼭 확인하세요. 감사합니다!"
    return script

def save_script_to_file(script, output_dir="output"):
    """스크립트를 텍스트 파일로 저장"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    today = datetime.datetime.now().strftime("%Y%m%d")
    file_path = os.path.join(output_dir, f"hotdeal_script_{today}.txt")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(script)
    
    return file_path

def main():
    # 환경 변수에서 설정 가져오기 (기본값 제공)
    min_discount = int(os.environ.get('MIN_DISCOUNT', 20))
    max_items = int(os.environ.get('MAX_ITEMS_PER_CATEGORY', 5))
    
    try:
        # 최신 CSV 파일 찾기
        csv_file = get_latest_csv_file()
        print(f"최신 핫딜 파일: {csv_file}")
        
        # 핫딜 데이터 읽기 (pandas DataFrame으로)
        deals_df = read_deals_from_csv(csv_file)
        print(f"총 {len(deals_df)}개의 상품 정보를 읽었습니다.")
        
        # 카테고리별 분류
        categorized = categorize_deals(deals_df, min_discount)
        print(f"총 {len(categorized)}개의 카테고리로 분류되었습니다.")
        
        # 스크립트 생성
        script = generate_full_script(categorized, max_items)
        
        # 파일로 저장
        output_file = save_script_to_file(script)
        print(f"스크립트가 생성되었습니다: {output_file}")
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()
