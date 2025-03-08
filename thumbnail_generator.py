#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import io
import glob
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance, ImageOps, ImageChops

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("thumbnail_generator.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ThumbnailGenerator")

# 상수 정의
THUMBNAIL_WIDTH = 1280
THUMBNAIL_HEIGHT = 720
ASSETS_DIR = os.path.join("assets")
FONTS_DIR = os.path.join(ASSETS_DIR, "fonts")
BACKGROUNDS_DIR = os.path.join(ASSETS_DIR, "backgrounds")
LOGO_DIR = os.path.join(ASSETS_DIR, "logo")
ICONS_DIR = os.path.join(ASSETS_DIR, "icons")
THUMBNAILS_DIR = "thumbnails"

# 폰트 경로
FONT_REGULAR = os.path.join(FONTS_DIR, "NanumGothic.ttf")
FONT_BOLD = os.path.join(FONTS_DIR, "NanumGothicBold.ttf")
FONT_EXTRA_BOLD = os.path.join(FONTS_DIR, "NanumGothicExtraBold.ttf")
FONT_SQUARE = os.path.join(FONTS_DIR, "NanumSquare_acB.ttf")
FONT_SQUARE_BOLD = os.path.join(FONTS_DIR, "NanumSquare_acEB.ttf")

# 기본 이미지 경로
DEFAULT_BACKGROUND = os.path.join(BACKGROUNDS_DIR, "thumbnail_bg.jpg")
DEFAULT_PRODUCT_IMAGE = os.path.join(ASSETS_DIR, "default_product.jpg")
LOGO_PATH = os.path.join(LOGO_DIR, "logo.png")
HOTDEAL_BADGE_PATH = os.path.join(ICONS_DIR, "hotdeal_badge.png")
SPARKLE_EFFECT_PATH = os.path.join(ICONS_DIR, "sparkle.png")

# 브랜드 색상
BRAND_COLORS = {
    "primary": "#FF3A30",       # 메인 빨강
    "secondary": "#212121",     # 보조 검정
    "gradient_start": "#FF3A30", # 그라데이션 시작 색상
    "gradient_end": "#8A0900",  # 그라데이션 끝 색상
    "alt_gradient_start": "#3A30FF", # 대체 그라데이션 시작 색상
    "alt_gradient_end": "#09008A",  # 대체 그라데이션 끝 색상
    "text_light": "#FFFFFF",    # 밝은 텍스트
    "text_dark": "#212121",     # 어두운 텍스트
    "accent": "#FFD700",        # 강조 금색
    "discount": "#FF3A30",      # 할인율 색상
    "price": "#00AA00",         # 가격 색상
    "shadow": "#00000080"       # 그림자색 (반투명 검정)
}

def ensure_directories_exist():
    """필요한 디렉토리가 존재하는지 확인하고 없으면 생성"""
    directories = [
        ASSETS_DIR, FONTS_DIR, BACKGROUNDS_DIR, LOGO_DIR, 
        ICONS_DIR, THUMBNAILS_DIR
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"디렉토리 생성: {directory}")

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

def get_top_discounted_products(deals_df, top_n=10):
    """할인율 기준으로 상위 상품 선별"""
    try:
        # 할인율 열이 있는지 확인
        if 'discount' not in deals_df.columns:
            logger.warning("할인율 열이 없습니다. 기본 열 사용")
            deals_df['discount'] = 0
        
        # 할인율 숫자로 변환 (예: '30%' -> 30)
        deals_df['discount_value'] = deals_df['discount'].apply(
            lambda x: int(str(x).strip('%')) if pd.notna(x) and str(x).strip('%').isdigit() else 0
        )
        
        # 할인율 기준 내림차순 정렬 및 상위 N개 선택
        top_products = deals_df.sort_values('discount_value', ascending=False).head(top_n)
        
        logger.info(f"상위 {len(top_products)}개 할인 상품 선별 완료")
        return top_products
    except Exception as e:
        logger.error(f"상위 할인 상품 선별 실패: {e}")
        return deals_df.head(top_n) if not deals_df.empty else pd.DataFrame()

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
        return Image.open(DEFAULT_PRODUCT_IMAGE)
    except Exception as e:
        logger.error(f"기본 이미지 로드 실패: {e}")
        # 완전히 실패한 경우 빈 이미지 생성
        return Image.new('RGB', (300, 300), color="#CCCCCC")

def create_gradient_background(width, height, start_color, end_color, direction="horizontal"):
    """그라데이션 배경 생성"""
    background = Image.new('RGB', (width, height), start_color)
    draw = ImageDraw.Draw(background)
    
    # 그라데이션 방향에 따라 처리
    if direction == "horizontal":
        for x in range(width):
            # 위치에 따른 색상 계산
            r = int(int(start_color[1:3], 16) + (int(end_color[1:3], 16) - int(start_color[1:3], 16)) * x / width)
            g = int(int(start_color[3:5], 16) + (int(end_color[3:5], 16) - int(start_color[3:5], 16)) * x / width)
            b = int(int(start_color[5:7], 16) + (int(end_color[5:7], 16) - int(start_color[5:7], 16)) * x / width)
            
            # 세로선 그리기
            draw.line([(x, 0), (x, height)], fill=(r, g, b))
    else:  # vertical
        for y in range(height):
            # 위치에 따른 색상 계산
            r = int(int(start_color[1:3], 16) + (int(end_color[1:3], 16) - int(start_color[1:3], 16)) * y / height)
            g = int(int(start_color[3:5], 16) + (int(end_color[3:5], 16) - int(start_color[3:5], 16)) * y / height)
            b = int(int(start_color[5:7], 16) + (int(end_color[5:7], 16) - int(start_color[5:7], 16)) * y / height)
            
            # 가로선 그리기
            draw.line([(0, y), (width, y)], fill=(r, g, b))
    
    return background

def add_noise_texture(image, intensity=10):
    """이미지에 노이즈 텍스처 추가"""
    # 원본 이미지 크기의 노이즈 이미지 생성
    noise = Image.new('RGB', image.size, (0, 0, 0))
    draw = ImageDraw.Draw(noise)
    
    # 랜덤 픽셀 생성
    for x in range(0, image.width, 2):
        for y in range(0, image.height, 2):
            # 랜덤 밝기의 회색 픽셀
            gray = np.random.randint(0, intensity)
            draw.point((x, y), fill=(gray, gray, gray))
    
    # 노이즈 이미지를 블러 처리하여 부드럽게
    noise = noise.filter(ImageFilter.GaussianBlur(1))
    
    # 원본 이미지와 노이즈 이미지 합성 (스크린 모드)
    return ImageChops.screen(image, noise)

def get_logo_image(size=(150, 150)):
    """로고 이미지 로드 및 크기 조정"""
    try:
        logo = Image.open(LOGO_PATH)
        logo = logo.resize(size, Image.LANCZOS)
        return logo
    except Exception as e:
        logger.warning(f"로고 이미지 로드 실패: {e}")
        return None

def get_hotdeal_badge(size=(200, 200)):
    """핫딜 배지 이미지 로드"""
    try:
        badge = Image.open(HOTDEAL_BADGE_PATH)
        badge = badge.resize(size, Image.LANCZOS)
        return badge
    except Exception as e:
        logger.warning(f"핫딜 배지 로드 실패: {e}, 기본 배지 생성")
        
        # 기본 배지 생성
        badge = Image.new('RGBA', size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(badge)
        
        # 원형 배지 그리기
        draw.ellipse([(0, 0), size], fill=BRAND_COLORS["primary"])
        
        # 텍스트 추가
        try:
            font = ImageFont.truetype(FONT_SQUARE_BOLD, size[0] // 4)
            text = "핫딜"
            text_width = draw.textlength(text, font=font)
            text_x = (size[0] - text_width) // 2
            text_y = (size[1] - font.size) // 2
            draw.text((text_x, text_y), text, font=font, fill="white")
        except Exception:
            # 폰트 로드 실패 시 기본 폰트 사용
            font = ImageFont.load_default()
            draw.text((size[0]//4, size[1]//3), "핫딜", font=font, fill="white")
        
        return badge

def get_sparkle_effect(size=(100, 100)):
    """스파클 효과 이미지 로드"""
    try:
        sparkle = Image.open(SPARKLE_EFFECT_PATH)
        sparkle = sparkle.resize(size, Image.LANCZOS)
        return sparkle
    except Exception as e:
        logger.warning(f"스파클 효과 로드 실패: {e}, 기본 효과 생성")
        
        # 기본 스파클 효과 생성
        sparkle = Image.new('RGBA', size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(sparkle)
        
        # 간단한 별 모양 그리기
        center_x, center_y = size[0] // 2, size[1] // 2
        points = []
        
        # 별 모양의 꼭지점 계산
        for i in range(10):
            radius = center_x * 0.9 if i % 2 == 0 else center_x * 0.4
            angle = i * 36 * (3.14159 / 180)  # 36도 간격, 라디안 변환
            x = center_x + radius * np.cos(angle)
            y = center_y + radius * np.sin(angle)
            points.append((x, y))
        
        # 별 그리기
        draw.polygon(points, fill=BRAND_COLORS["accent"])
        
        return sparkle

def load_fonts():
    """폰트 로드"""
    try:
        fonts = {
            "title": ImageFont.truetype(FONT_SQUARE_BOLD, 80),
            "subtitle": ImageFont.truetype(FONT_SQUARE, 50),
            "date": ImageFont.truetype(FONT_SQUARE, 40),
            "discount": ImageFont.truetype(FONT_SQUARE_BOLD, 120),
            "percent": ImageFont.truetype(FONT_SQUARE_BOLD, 60),
            "price": ImageFont.truetype(FONT_BOLD, 36),
            "regular": ImageFont.truetype(FONT_REGULAR, 30)
        }
        return fonts
    except Exception as e:
        logger.error(f"폰트 로드 실패: {e}")
        # 기본 폰트 사용
        return {
            "title": ImageFont.load_default(),
            "subtitle": ImageFont.load_default(),
            "date": ImageFont.load_default(),
            "discount": ImageFont.load_default(),
            "percent": ImageFont.load_default(),
            "price": ImageFont.load_default(),
            "regular": ImageFont.load_default()
        }

def add_text_with_shadow(draw, text, position, font, text_color, shadow_color="#00000080", shadow_offset=(3, 3)):
    """그림자 효과가 있는 텍스트 추가"""
    x, y = position
    
    # 그림자 텍스트
    draw.text((x + shadow_offset[0], y + shadow_offset[1]), text, font=font, fill=shadow_color)
    
    # 메인 텍스트
    draw.text((x, y), text, font=font, fill=text_color)

def add_text_with_outline(draw, text, position, font, text_color, outline_color="#000000", outline_width=2):
    """외곽선 효과가 있는 텍스트 추가"""
    x, y = position
    
    # 외곽선 효과 (8방향으로 텍스트 그리기)
    for offset_x in range(-outline_width, outline_width + 1):
        for offset_y in range(-outline_width, outline_width + 1):
            if offset_x == 0 and offset_y == 0:
                continue  # 중앙 위치는 건너뛰기
            draw.text((x + offset_x, y + offset_y), text, font=font, fill=outline_color)
    
    # 메인 텍스트
    draw.text((x, y), text, font=font, fill=text_color)

def create_product_collage(product_images, width, height, margin=10):
    """상품 이미지 콜라주 생성"""
    # 콜라주 배경 생성
    collage = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    
    if not product_images:
        return collage
    
    # 이미지 수에 따라 레이아웃 결정
    num_images = len(product_images)
    
    if num_images == 1:
        # 단일 이미지 - 중앙에 크게 배치
        img = product_images[0].copy()
        img = img.resize((width - 2*margin, height - 2*margin), Image.LANCZOS)
        collage.paste(img, (margin, margin), img if img.mode == 'RGBA' else None)
    
    elif num_images == 2:
        # 2개 이미지 - 좌우로 배치
        img_width = (width - 3*margin) // 2
        img_height = height - 2*margin
        
        for i, img in enumerate(product_images):
            img = img.copy()
            img = img.resize((img_width, img_height), Image.LANCZOS)
            x = margin + i * (img_width + margin)
            collage.paste(img, (x, margin), img if img.mode == 'RGBA' else None)
    
    elif num_images == 3:
        # 3개 이미지 - 좌측 큰 이미지, 우측 상하 2개 이미지
        left_width = (width - 3*margin) // 2
        right_width = left_width
        left_height = height - 2*margin
        right_height = (height - 3*margin) // 2
        
        # 좌측 큰 이미지
        img = product_images[0].copy()
        img = img.resize((left_width, left_height), Image.LANCZOS)
        collage.paste(img, (margin, margin), img if img.mode == 'RGBA' else None)
        
        # 우측 상하 이미지
        for i in range(1, 3):
            img = product_images[i].copy()
            img = img.resize((right_width, right_height), Image.LANCZOS)
            x = margin * 2 + left_width
            y = margin + (i-1) * (right_height + margin)
            collage.paste(img, (x, y), img if img.mode == 'RGBA' else None)
    
    elif num_images >= 4:
        # 4개 이상 이미지 - 2x2 그리드로 배치 (최대 4개만 사용)
        img_width = (width - 3*margin) // 2
        img_height = (height - 3*margin) // 2
        
        for i in range(min(4, num_images)):
            img = product_images[i].copy()
            img = img.resize((img_width, img_height), Image.LANCZOS)
            x = margin + (i % 2) * (img_width + margin)
            y = margin + (i // 2) * (img_height + margin)
            collage.paste(img, (x, y), img if img.mode == 'RGBA' else None)
    
    return collage

def create_thumbnail(top_products, output_path, use_alt_gradient=False):
    """썸네일 이미지 생성"""
    # 폰트 로드
    fonts = load_fonts()
    
    # 그라데이션 배경 생성
    if use_alt_gradient:
        background = create_gradient_background(
            THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, 
            BRAND_COLORS["alt_gradient_start"], 
            BRAND_COLORS["alt_gradient_end"],
            "horizontal"
        )
    else:
        background = create_gradient_background(
            THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT, 
            BRAND_COLORS["gradient_start"], 
            BRAND_COLORS["gradient_end"],
            "horizontal"
        )
    
    # 배경에 노이즈 텍스처 추가
    background = add_noise_texture(background)
    
    # 반투명 오버레이 추가 (가독성 향상)
    overlay = Image.new('RGBA', (THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    
    # 상단 어두운 그라데이션 (텍스트 가독성 향상)
    for y in range(200):
        alpha = 150 - y // 2  # 위에서 아래로 투명해지는 그라데이션
        if alpha < 0:
            alpha = 0
        overlay_draw.line([(0, y), (THUMBNAIL_WIDTH, y)], fill=(0, 0, 0, alpha))
    
    # 하단 어두운 그라데이션 (텍스트 가독성 향상)
    for y in range(200):
        alpha = y // 2  # 아래에서 위로 투명해지는 그라데이션
        if alpha < 0:
            alpha = 0
        overlay_draw.line([(0, THUMBNAIL_HEIGHT - y - 1), (THUMBNAIL_WIDTH, THUMBNAIL_HEIGHT - y - 1)], fill=(0, 0, 0, alpha))
    
    # 배경에 오버레이 합성
    background = Image.alpha_composite(background.convert('RGBA'), overlay)
    
    # 상품 이미지 다운로드 및 처리
    product_images = []
    max_discount = 0
    
    for _, product in top_products.iterrows():
        # 최대 할인율 업데이트
        discount = product.get('discount_value', 0)
        if discount > max_discount:
            max_discount = discount
        
        # 이미지 URL이 있는 경우 다운로드
        if 'image_url' in product and product['image_url']:
            img = download_image(product['image_url'])
            if img:
                # 이미지를 RGBA 모드로 변환
                if img.mode != 'RGBA':
                    img = img.convert('RGBA')
                
                # 이미지 테두리 추가
                bordered_img = ImageOps.expand(img, border=5, fill='white')
                
                # 이미지 그림자 효과 추가
                shadow_img = Image.new('RGBA', bordered_img.size, (0, 0, 0, 0))
                shadow_draw = ImageDraw.Draw(shadow_img)
                shadow_draw.rectangle([(0, 0), bordered_img.size], fill=(0, 0, 0, 100))
                shadow_img = shadow_img.filter(ImageFilter.GaussianBlur(10))
                
                # 그림자와 이미지 합성
                combined = Image.new('RGBA', bordered_img.size, (0, 0, 0, 0))
                combined.paste(shadow_img, (10, 10))
                combined.paste(bordered_img, (0, 0), bordered_img)
                
                product_images.append(combined)
        
        # 필요한 수의 이미지만 사용
        if len(product_images) >= 4:
            break
    
    # 이미지가 부족한 경우 기본 이미지로 채우기
    while len(product_images) < 4:
        default_img = get_default_image()
        if default_img.mode != 'RGBA':
            default_img = default_img.convert('RGBA')
        product_images.append(default_img)
    
    # 상품 이미지 콜라주 생성
    collage_width = THUMBNAIL_WIDTH // 2
    collage_height = THUMBNAIL_HEIGHT - 200
    collage = create_product_collage(product_images, collage_width, collage_height)
    
    # 콜라주를 배경에 합성
    background.paste(collage, (THUMBNAIL_WIDTH - collage_width - 50, 100), collage)
    
    # 로고 추가
    logo = get_logo_image((150, 150))
    if logo:
        background.paste(logo, (30, 30), logo if logo.mode == 'RGBA' else None)
    
    # 핫딜 배지 추가
    badge = get_hotdeal_badge((180, 180))
    if badge:
        background.paste(badge, (THUMBNAIL_WIDTH - 200, THUMBNAIL_HEIGHT - 200), badge)
    
    # 최대 할인율 표시
    if max_discount > 0:
        # 할인율 배경 원
        draw = ImageDraw.Draw(background)
        discount_circle_x = 300
        discount_circle_y = 350
        discount_circle_radius = 120
        
        # 원 그리기 (그림자 효과)
        draw.ellipse(
            [(discount_circle_x - discount_circle_radius + 5, discount_circle_y - discount_circle_radius + 5),
             (discount_circle_x + discount_circle_radius + 5, discount_circle_y + discount_circle_radius + 5)],
            fill="#00000080"
        )
        
        # 원 그리기 (메인)
        draw.ellipse(
            [(discount_circle_x - discount_circle_radius, discount_circle_y - discount_circle_radius),
             (discount_circle_x + discount_circle_radius, discount_circle_y + discount_circle_radius)],
            fill=BRAND_COLORS["discount"]
        )
        
        # 할인율 텍스트
        discount_text = f"{max_discount}"
        discount_width = draw.textlength(discount_text, font=fonts["discount"])
        
        # 할인율 숫자
        add_text_with_shadow(
            draw,
            discount_text,
            (discount_circle_x - discount_width // 2, discount_circle_y - 60),
            fonts["discount"],
            "white",
            "#00000080",
            (3, 3)
        )
        
        # % 기호
        percent_text = "%"
        percent_width = draw.textlength(percent_text, font=fonts["percent"])
        add_text_with_shadow(
            draw,
            percent_text,
            (discount_circle_x - percent_width // 2, discount_circle_y + 20),
            fonts["percent"],
            "white",
            "#00000080",
            (2, 2)
        )
        
        # 최대 할인 텍스트
        max_text = "최대"
        max_width = draw.textlength(max_text, font=fonts["regular"])
        add_text_with_shadow(
            draw,
            max_text,
            (discount_circle_x - max_width // 2, discount_circle_y - 100),
            fonts["regular"],
            "white",
            "#00000080",
            (2, 2)
        )
        
        # 스파클 효과 추가
        sparkle = get_sparkle_effect((80, 80))
        if sparkle:
            background.paste(sparkle, (discount_circle_x + discount_circle_radius - 40, discount_circle_y - discount_circle_radius - 20), sparkle)
            background.paste(sparkle, (discount_circle_x - discount_circle_radius - 40, discount_circle_y + discount_circle_radius - 40), sparkle)
    
    # 제목 텍스트 추가
    draw = ImageDraw.Draw(background)
    
    # 메인 제목
    title_text = "오늘의 쿠팡 핫딜 TOP 10"
    add_text_with_outline(
        draw,
        title_text,
        (50, 180),
        fonts["title"],
        "white",
        "black",
        3
    )
    
    # 날짜 표시
    date_text = datetime.now().strftime("%Y년 %m월 %d일")
    add_text_with_shadow(
        draw,
        date_text,
        (50, 280),
        fonts["date"],
        "white",
        "#00000080",
        (3, 3)
    )
    
    # 추가 설명 텍스트
    subtitle_text = "놓치면 후회할 특가 상품"
    add_text_with_shadow(
        draw,
        subtitle_text,
        (50, 350),
        fonts["subtitle"],
        BRAND_COLORS["accent"],
        "#00000080",
        (3, 3)
    )
    
    # 썸네일 저장
    background = background.convert('RGB')
    background.save(output_path, quality=95)
    logger.info(f"썸네일 생성 완료: {output_path}")
    
    return output_path

def generate_video_title(max_discount, date=None):
    """SEO 최적화된 영상 타 