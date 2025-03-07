#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import io
import csv
import glob
import json
import logging
import requests
import textwrap
import pandas as pd
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("video_frame_generator.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VideoFrameGenerator")

# 상수 정의
FRAME_WIDTH = 1920
FRAME_HEIGHT = 1080
FONT_PATH = os.path.join("assets", "fonts", "NanumGothic.ttf")
BACKGROUND_PATH = os.path.join("assets", "backgrounds", "background.jpg")
LOGO_PATH = os.path.join("assets", "logo", "logo.png")
DEFAULT_IMAGE_PATH = os.path.join("assets", "default_product.jpg")

# 카테고리별 색상 스키마
CATEGORY_COLORS = {
    "식품": {"primary": "#FF5733", "secondary": "#FFC300", "text": "#FFFFFF"},
    "생활용품": {"primary": "#33A8FF", "secondary": "#33FFEC", "text": "#000000"},
    "전자제품": {"primary": "#3357FF", "secondary": "#33FFA8", "text": "#FFFFFF"},
    "패션": {"primary": "#FF33A8", "secondary": "#FF33EC", "text": "#FFFFFF"},
    "가구/인테리어": {"primary": "#A833FF", "secondary": "#EC33FF", "text": "#FFFFFF"},
    "뷰티": {"primary": "#FF33EC", "secondary": "#FF5733", "text": "#FFFFFF"},
    "스포츠/레저": {"primary": "#33FF57", "secondary": "#33FFA8", "text": "#000000"},
    "디지털/가전": {"primary": "#3357FF", "secondary": "#33A8FF", "text": "#FFFFFF"},
    "도서": {"primary": "#A8FF33", "secondary": "#ECFF33", "text": "#000000"},
    "완구/취미": {"primary": "#FF5733", "secondary": "#FFC300", "text": "#FFFFFF"},
    "기타": {"primary": "#808080", "secondary": "#C0C0C0", "text": "#FFFFFF"}
}

def get_latest_csv_file(directory="data"):
    """최신 핫딜 CSV 파일 경로 반환"""
    pattern = os.path.join(directory, "coupang_deals_*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"'{pattern}' 패턴과 일치하는 파일을 찾을 수 없습니다.")
    return max(files, key=os.path.getctime)

def read_deals_from_csv(file_path):
    """CSV 파일에서 핫딜 상품 정보 읽기"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"CSV 파일 읽기 성공: {len(df)}개 상품 정보 로드")
        return df
    except Exception as e:
        logger.error(f"CSV 파일 읽기 실패: {e}")
        return pd.DataFrame()

def download_image(url, timeout=10):
    """URL에서 이미지 다운로드"""
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content))
        return image
    except Exception as e:
        logger.warning(f"이미지 다운로드 실패: {url}, 오류: {e}")
        return None

def get_default_image():
    """기본 이미지 로드"""
    try:
        return Image.open(DEFAULT_IMAGE_PATH)
    except Exception as e:
        logger.error(f"기본 이미지 로드 실패: {e}")
        # 완전히 실패한 경우 빈 이미지 생성
        return Image.new('RGB', (500, 500), color='#CCCCCC')

def get_background_image():
    """배경 이미지 로드"""
    try:
        return Image.open(BACKGROUND_PATH).resize((FRAME_WIDTH, FRAME_HEIGHT))
    except Exception as e:
        logger.warning(f"배경 이미지 로드 실패: {e}, 기본 배경 생성")
        return Image.new('RGB', (FRAME_WIDTH, FRAME_HEIGHT), color='#F5F5F5')

def get_logo_image():
    """로고 이미지 로드"""
    try:
        return Image.open(LOGO_PATH)
    except Exception as e:
        logger.warning(f"로고 이미지 로드 실패: {e}")
        return None

def load_fonts():
    """폰트 로드"""
    try:
        title_font = ImageFont.truetype(FONT_PATH, 60)
        subtitle_font = ImageFont.truetype(FONT_PATH, 48)
        price_font = ImageFont.truetype(FONT_PATH, 72)
        discount_font = ImageFont.truetype(FONT_PATH, 90)
        regular_font = ImageFont.truetype(FONT_PATH, 36)
        small_font = ImageFont.truetype(FONT_PATH, 24)
        
        return {
            "title": title_font,
            "subtitle": subtitle_font,
            "price": price_font,
            "discount": discount_font,
            "regular": regular_font,
            "small": small_font
        }
    except Exception as e:
        logger.error(f"폰트 로드 실패: {e}")
        # 기본 폰트 사용
        return {
            "title": ImageFont.load_default(),
            "subtitle": ImageFont.load_default(),
            "price": ImageFont.load_default(),
            "discount": ImageFont.load_default(),
            "regular": ImageFont.load_default(),
            "small": ImageFont.load_default()
        }

def create_intro_frame(fonts, timestamp):
    """인트로 프레임 생성"""
    background = get_background_image()
    draw = ImageDraw.Draw(background)
    
    # 타이틀 텍스트
    title_text = "오늘의 핫딜 정보"
    subtitle_text = datetime.now().strftime("%Y년 %m월 %d일")
    
    # 타이틀 위치 계산
    title_width = draw.textlength(title_text, font=fonts["title"])
    title_x = (FRAME_WIDTH - title_width) // 2
    
    # 서브타이틀 위치 계산
    subtitle_width = draw.textlength(subtitle_text, font=fonts["subtitle"])
    subtitle_x = (FRAME_WIDTH - subtitle_width) // 2
    
    # 텍스트 그리기
    draw.text((title_x, 400), title_text, font=fonts["title"], fill="#FFFFFF")
    draw.text((subtitle_x, 500), subtitle_text, font=fonts["subtitle"], fill="#FFFFFF")
    
    # 로고 추가
    logo = get_logo_image()
    if logo:
        logo = logo.resize((300, 300), Image.LANCZOS)
        background.paste(logo, (FRAME_WIDTH // 2 - 150, 100), logo if logo.mode == 'RGBA' else None)
    
    # 메타데이터 추가
    metadata = {
        "type": "intro",
        "timestamp": timestamp,
        "timecode": "00:00:00"
    }
    
    return background, metadata

def create_category_intro_frame(category, fonts, timestamp, frame_index):
    """카테고리 인트로 프레임 생성"""
    background = get_background_image()
    draw = ImageDraw.Draw(background)
    
    # 카테고리 색상 가져오기
    colors = CATEGORY_COLORS.get(category, CATEGORY_COLORS["기타"])
    
    # 반투명 오버레이 생성
    overlay = Image.new('RGBA', (FRAME_WIDTH, FRAME_HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    
    # 상단 배너
    overlay_draw.rectangle([(0, 100), (FRAME_WIDTH, 300)], fill=(int(colors["primary"][1:3], 16), 
                                                                int(colors["primary"][3:5], 16), 
                                                                int(colors["primary"][5:7], 16), 200))
    
    # 카테고리 텍스트
    category_text = f"{category} 핫딜"
    text_width = draw.textlength(category_text, font=fonts["title"])
    text_x = (FRAME_WIDTH - text_width) // 2
    
    # 배경에 오버레이 합성
    background = Image.alpha_composite(background.convert('RGBA'), overlay).convert('RGB')
    draw = ImageDraw.Draw(background)
    
    # 텍스트 그리기
    draw.text((text_x, 180), category_text, font=fonts["title"], fill=colors["text"])
    
    # 설명 텍스트
    description = f"오늘의 {category} 핫딜 상품을 소개합니다"
    desc_width = draw.textlength(description, font=fonts["regular"])
    desc_x = (FRAME_WIDTH - desc_width) // 2
    draw.text((desc_x, 400), description, font=fonts["regular"], fill="#FFFFFF")
    
    # 메타데이터 추가
    metadata = {
        "type": "category_intro",
        "category": category,
        "timestamp": timestamp,
        "frame_index": frame_index,
        "timecode": f"00:{frame_index:02d}:00"
    }
    
    return background, metadata

def create_product_frame(product, category, fonts, timestamp, frame_index, product_index):
    """상품 정보 프레임 생성"""
    background = get_background_image()
    draw = ImageDraw.Draw(background)
    
    # 카테고리 색상 가져오기
    colors = CATEGORY_COLORS.get(category, CATEGORY_COLORS["기타"])
    
    # 반투명 오버레이 생성
    overlay = Image.new('RGBA', (FRAME_WIDTH, FRAME_HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    
    # 상단 배너
    overlay_draw.rectangle([(0, 50), (FRAME_WIDTH, 150)], fill=(int(colors["primary"][1:3], 16), 
                                                               int(colors["primary"][3:5], 16), 
                                                               int(colors["primary"][5:7], 16), 200))
    
    # 상품 정보 영역
    overlay_draw.rectangle([(100, 200), (FRAME_WIDTH - 100, FRAME_HEIGHT - 100)], 
                          fill=(255, 255, 255, 200))
    
    # 배경에 오버레이 합성
    background = Image.alpha_composite(background.convert('RGBA'), overlay).convert('RGB')
    draw = ImageDraw.Draw(background)
    
    # 카테고리 텍스트
    draw.text((50, 80), f"{category} 핫딜", font=fonts["subtitle"], fill=colors["text"])
    
    # 상품 이미지 다운로드 및 배치
    product_image = None
    if 'image_url' in product and product['image_url']:
        product_image = download_image(product['image_url'])
    
    if product_image is None:
        product_image = get_default_image()
    
    # 이미지 크기 조정 및 배치
    product_image = product_image.resize((500, 500), Image.LANCZOS)
    background.paste(product_image, (150, 250))
    
    # 상품명 (긴 경우 줄바꿈)
    title = product.get('title', '제품명 없음')
    wrapped_title = textwrap.fill(title, width=30)
    draw.text((700, 250), wrapped_title, font=fonts["subtitle"], fill="#000000")
    
    # 가격 정보
    price = product.get('price', '가격 정보 없음')
    original_price = product.get('original_price', '')
    
    if original_price:
        draw.text((700, 450), f"정가: {original_price}", font=fonts["regular"], fill="#888888")
        draw.line([(700, 470), (700 + draw.textlength(f"정가: {original_price}", font=fonts["regular"]), 470)], 
                 fill="#888888", width=3)
    
    draw.text((700, 500), f"{price}", font=fonts["price"], fill="#FF0000")
    
    # 할인율
    discount = product.get('discount', 0)
    if discount:
        # 할인율 배지 그리기
        badge_size = 150
        overlay_draw = ImageDraw.Draw(background)
        overlay_draw.ellipse([(FRAME_WIDTH - 300, 250), (FRAME_WIDTH - 300 + badge_size, 250 + badge_size)], 
                           fill=colors["secondary"])
        
        # 할인율 텍스트
        discount_text = f"{discount}%"
        discount_width = draw.textlength(discount_text, font=fonts["discount"])
        draw.text((FRAME_WIDTH - 300 + (badge_size - discount_width) // 2, 275), 
                 discount_text, font=fonts["discount"], fill="#000000")
        
        draw.text((700, 600), "할인", font=fonts["regular"], fill="#000000")
    
    # 상품 URL QR 코드 (여기서는 생략, 필요시 qrcode 라이브러리 사용)
    
    # 메타데이터 추가
    metadata = {
        "type": "product",
        "category": category,
        "product_id": product.get('id', ''),
        "product_title": title,
        "timestamp": timestamp,
        "frame_index": frame_index,
        "product_index": product_index,
        "timecode": f"00:{frame_index:02d}:{product_index:02d}"
    }
    
    return background, metadata

def create_outro_frame(fonts, timestamp, frame_index):
    """아웃트로 프레임 생성"""
    background = get_background_image()
    draw = ImageDraw.Draw(background)
    
    # 타이틀 텍스트
    title_text = "이상 오늘의 핫딜 정보였습니다"
    subtitle_text = "구매하실 때는 가격 변동이 있을 수 있으니 최종 가격을 꼭 확인하세요"
    
    # 타이틀 위치 계산
    title_width = draw.textlength(title_text, font=fonts["title"])
    title_x = (FRAME_WIDTH - title_width) // 2
    
    # 서브타이틀 위치 계산
    subtitle_width = draw.textlength(subtitle_text, font=fonts["regular"])
    subtitle_x = (FRAME_WIDTH - subtitle_width) // 2
    
    # 텍스트 그리기
    draw.text((title_x, 400), title_text, font=fonts["title"], fill="#FFFFFF")
    draw.text((subtitle_x, 500), subtitle_text, font=fonts["regular"], fill="#FFFFFF")
    
    # 로고 추가
    logo = get_logo_image()
    if logo:
        logo = logo.resize((200, 200), Image.LANCZOS)
        background.paste(logo, (FRAME_WIDTH // 2 - 100, 600), logo if logo.mode == 'RGBA' else None)
    
    # 메타데이터 추가
    metadata = {
        "type": "outro",
        "timestamp": timestamp,
        "frame_index": frame_index,
        "timecode": f"00:{frame_index:02d}:00"
    }
    
    return background, metadata

def categorize_deals(deals, min_discount=20):
    """상품을 카테고리별로 분류하고 할인율 높은 순으로 정렬"""
    categorized = {}
    
    for _, deal in deals.iterrows():
        category = deal.get('category', '기타')
        if not category or pd.isna(category):
            category = '기타'
            
        # 할인율 확인
        discount = 0
        if 'discount' in deal and not pd.isna(deal['discount']):
            discount_str = str(deal['discount']).strip('%')
            try:
                discount = int(discount_str)
            except ValueError:
                discount = 0
        
        if discount >= min_discount:
            if category not in categorized:
                categorized[category] = []
            
            categorized[category].append({
                'id': deal.get('id', ''),
                'title': deal.get('title', '제목 없음'),
                'price': deal.get('price', '가격 정보 없음'),
                'original_price': deal.get('original_price', ''),
                'discount': discount,
                'image_url': deal.get('image_url', ''),
                'url': deal.get('url', '')
            })
    
    # 각 카테고리 내에서 할인율 높은 순으로 정렬
    for category in categorized:
        categorized[category].sort(key=lambda x: x['discount'], reverse=True)
    
    return categorized

def generate_frames(categorized_deals, output_dir="frames", max_items_per_category=5):
    """모든 프레임 생성 및 저장"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 출력 디렉토리 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"출력 디렉토리 생성: {output_dir}")
    
    # 메타데이터 저장 디렉토리
    metadata_dir = os.path.join(output_dir, "metadata")
    if not os.path.exists(metadata_dir):
        os.makedirs(metadata_dir)
    
    # 폰트 로드
    fonts = load_fonts()
    
    # 생성된 프레임 정보
    frames_info = []
    frame_index = 1
    
    # 인트로 프레임 생성
    intro_frame, intro_metadata = create_intro_frame(fonts, timestamp)
    intro_filename = os.path.join(output_dir, f"{timestamp}_00_intro.jpg")
    intro_frame.save(intro_filename, quality=95)
    
    # 메타데이터 저장
    with open(os.path.join(metadata_dir, f"{timestamp}_00_intro.json"), 'w', encoding='utf-8') as f:
        json.dump(intro_metadata, f, ensure_ascii=False, indent=2)
    
    frames_info.append({
        "filename": intro_filename,
        "metadata": intro_metadata
    })
    
    logger.info(f"인트로 프레임 생성 완료: {intro_filename}")
    
    # 카테고리별 프레임 생성
    for category, products in categorized_deals.items():
        if not products:
            continue
        
        # 카테고리 인트로 프레임
        category_frame, category_metadata = create_category_intro_frame(category, fonts, timestamp, frame_index)
        category_filename = os.path.join(output_dir, f"{timestamp}_{frame_index:02d}_category_{category}.jpg")
        category_frame.save(category_filename, quality=95)
        
        # 메타데이터 저장
        with open(os.path.join(metadata_dir, f"{timestamp}_{frame_index:02d}_category_{category}.json"), 'w', encoding='utf-8') as f:
            json.dump(category_metadata, f, ensure_ascii=False, indent=2)
        
        frames_info.append({
            "filename": category_filename,
            "metadata": category_metadata
        })
        
        logger.info(f"카테고리 인트로 프레임 생성 완료: {category_filename}")
        
        frame_index += 1
        
        # 상품별 프레임 생성
        for product_index, product in enumerate(products[:max_items_per_category], 1):
            product_frame, product_metadata = create_product_frame(
                product, category, fonts, timestamp, frame_index, product_index
            )
            product_filename = os.path.join(output_dir, f"{timestamp}_{frame_index:02d}_{product_index:02d}_{category}_product.jpg")
            product_frame.save(product_filename, quality=95)
            
            # 메타데이터 저장
            with open(os.path.join(metadata_dir, f"{timestamp}_{frame_index:02d}_{product_index:02d}_{category}_product.json"), 'w', encoding='utf-8') as f:
                json.dump(product_metadata, f, ensure_ascii=False, indent=2)
            
            frames_info.append({
                "filename": product_filename,
                "metadata": product_metadata
            })
            
            logger.info(f"상품 프레임 생성 완료: {product_filename}")
        
        frame_index += 1
    
    # 아웃트로 프레임 생성
    outro_frame, outro_metadata = create_outro_frame(fonts, timestamp, frame_index)
    outro_filename = os.path.join(output_dir, f"{timestamp}_{frame_index:02d}_outro.jpg")
    outro_frame.save(outro_filename, quality=95)
    
    # 메타데이터 저장
    with open(os.path.join(metadata_dir, f"{timestamp}_{frame_index:02d}_outro.json"), 'w', encoding='utf-8') as f:
        json.dump(outro_metadata, f, ensure_ascii=False, indent=2)
    
    frames_info.append({
        "filename": outro_filename,
        "metadata": outro_metadata
    })
    
    logger.info(f"아웃트로 프레임 생성 완료: {outro_filename}")
    
    # 전체 프레임 정보 저장
    with open(os.path.join(metadata_dir, f"{timestamp}_frames_info.json"), 'w', encoding='utf-8') as f:
        json.dump(frames_info, f, ensure_ascii=False, indent=2)
    
    return frames_info

def main():
    try:
        # 환경 변수에서 설정 가져오기 (기본값 제공)
        min_discount = int(os.environ.get('MIN_DISCOUNT', 20))
        max_items = int(os.environ.get('MAX_ITEMS_PER_CATEGORY', 5))
        output_dir = os.environ.get('FRAMES_OUTPUT_DIR', 'frames')
        
        # 최신 CSV 파일 찾기
        csv_file = get_latest_csv_file()
        logger.info(f"최신 핫딜 파일: {csv_file}")
        
        # 핫딜 데이터 읽기
        deals = read_deals_from_csv(csv_file)
        if deals.empty:
            logger.error("핫딜 데이터가 비어있습니다.")
            return
        
        logger.info(f"총 {len(deals)}개의 상품 정보를 읽었습니다.")
        
        # 카테고리별 분류
        categorized = categorize_deals(deals, min_discount)
        logger.info(f"총 {len(categorized)}개의 카테고리로 분류되었습니다.")
        
        # 프레임 생성
        frames_info = generate_frames(categorized, output_dir, max_items)
        logger.info(f"총 {len(frames_info)}개의 프레임이 생성되었습니다.")
        
    except FileNotFoundError as e:
        logger.error(f"파일을 찾을 수 없습니다: {e}")
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main() 