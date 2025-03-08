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
import qrcode
import pandas as pd
import numpy as np
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance, ImageOps

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
ASSETS_DIR = os.path.join("assets")
FONTS_DIR = os.path.join(ASSETS_DIR, "fonts")
BACKGROUNDS_DIR = os.path.join(ASSETS_DIR, "backgrounds")
LOGO_DIR = os.path.join(ASSETS_DIR, "logo")
ICONS_DIR = os.path.join(ASSETS_DIR, "icons")
PATTERNS_DIR = os.path.join(ASSETS_DIR, "patterns")

# 폰트 경로
FONT_REGULAR = os.path.join(FONTS_DIR, "NanumGothic.ttf")
FONT_BOLD = os.path.join(FONTS_DIR, "NanumGothicBold.ttf")
FONT_EXTRA_BOLD = os.path.join(FONTS_DIR, "NanumGothicExtraBold.ttf")
FONT_SQUARE = os.path.join(FONTS_DIR, "NanumSquare_acB.ttf")
FONT_SQUARE_BOLD = os.path.join(FONTS_DIR, "NanumSquare_acEB.ttf")

# 기본 이미지 경로
DEFAULT_BACKGROUND = os.path.join(BACKGROUNDS_DIR, "background.jpg")
DEFAULT_PRODUCT_IMAGE = os.path.join(ASSETS_DIR, "default_product.jpg")
LOGO_PATH = os.path.join(LOGO_DIR, "logo.png")
PATTERN_PATH = os.path.join(PATTERNS_DIR, "pattern.png")

# 배송 아이콘 경로
FREE_SHIPPING_ICON = os.path.join(ICONS_DIR, "free_shipping.png")
ROCKET_SHIPPING_ICON = os.path.join(ICONS_DIR, "rocket_shipping.png")

# 브랜드 색상
BRAND_COLORS = {
    "primary": "#FF3A30",       # 메인 빨강
    "secondary": "#212121",     # 보조 검정
    "tertiary": "#F5F5F5",      # 밝은 회색
    "accent": "#FFD700",        # 강조 금색
    "text_dark": "#212121",     # 어두운 텍스트
    "text_light": "#FFFFFF",    # 밝은 텍스트
    "background": "#F8F8F8",    # 배경색
    "shadow": "#00000080"       # 그림자색 (반투명 검정)
}

# 카테고리별 색상 스키마
CATEGORY_COLORS = {
    "식품": {"primary": "#FF5733", "secondary": "#FFC300", "icon": "food.png"},
    "생활용품": {"primary": "#33A8FF", "secondary": "#33FFEC", "icon": "household.png"},
    "전자제품": {"primary": "#3357FF", "secondary": "#33FFA8", "icon": "electronics.png"},
    "패션": {"primary": "#FF33A8", "secondary": "#FF33EC", "icon": "fashion.png"},
    "가구/인테리어": {"primary": "#A833FF", "secondary": "#EC33FF", "icon": "furniture.png"},
    "뷰티": {"primary": "#FF33EC", "secondary": "#FF5733", "icon": "beauty.png"},
    "스포츠/레저": {"primary": "#33FF57", "secondary": "#33FFA8", "icon": "sports.png"},
    "디지털/가전": {"primary": "#3357FF", "secondary": "#33A8FF", "icon": "digital.png"},
    "도서": {"primary": "#A8FF33", "secondary": "#ECFF33", "icon": "books.png"},
    "완구/취미": {"primary": "#FF5733", "secondary": "#FFC300", "icon": "toys.png"},
    "기타": {"primary": "#808080", "secondary": "#C0C0C0", "icon": "etc.png"}
}

def ensure_directories_exist():
    """필요한 디렉토리가 존재하는지 확인하고 없으면 생성"""
    directories = [
        ASSETS_DIR, FONTS_DIR, BACKGROUNDS_DIR, LOGO_DIR, 
        ICONS_DIR, PATTERNS_DIR, "data", "output", "frames", 
        os.path.join("frames", "metadata"), "thumbnails"
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
        return Image.new('RGB', (500, 500), color=BRAND_COLORS["tertiary"])

def get_background_image():
    """배경 이미지 로드 및 처리"""
    try:
        # 기본 배경 이미지 로드
        background = Image.open(DEFAULT_BACKGROUND).resize((FRAME_WIDTH, FRAME_HEIGHT))
        
        # 배경 이미지 어둡게 처리 (영상 콘텐츠가 잘 보이도록)
        enhancer = ImageEnhance.Brightness(background)
        background = enhancer.enhance(0.7)
        
        # 패턴 오버레이 추가 (선택적)
        try:
            pattern = Image.open(PATTERN_PATH).resize((FRAME_WIDTH, FRAME_HEIGHT))
            pattern = pattern.convert("RGBA")
            # 패턴 투명도 조정
            pattern.putalpha(50)  # 0-255 범위, 낮을수록 투명
            
            # 배경에 패턴 합성
            background = background.convert("RGBA")
            background = Image.alpha_composite(background, pattern)
            background = background.convert("RGB")
        except Exception as e:
            logger.warning(f"패턴 오버레이 추가 실패: {e}")
        
        return background
    except Exception as e:
        logger.warning(f"배경 이미지 로드 실패: {e}, 기본 배경 생성")
        # 그라데이션 배경 생성
        background = Image.new('RGB', (FRAME_WIDTH, FRAME_HEIGHT), color=BRAND_COLORS["background"])
        draw = ImageDraw.Draw(background)
        
        # 상단에서 하단으로 그라데이션 효과
        for y in range(FRAME_HEIGHT):
            # 상단은 어두운 색, 하단은 밝은 색
            r = int(33 + (y / FRAME_HEIGHT) * 50)
            g = int(33 + (y / FRAME_HEIGHT) * 50)
            b = int(33 + (y / FRAME_HEIGHT) * 50)
            draw.line([(0, y), (FRAME_WIDTH, y)], fill=(r, g, b))
            
        return background

def get_logo_image(size=(300, 300)):
    """로고 이미지 로드 및 크기 조정"""
    try:
        logo = Image.open(LOGO_PATH)
        logo = logo.resize(size, Image.LANCZOS)
        return logo
    except Exception as e:
        logger.warning(f"로고 이미지 로드 실패: {e}")
        return None

def get_category_icon(category, size=(80, 80)):
    """카테고리 아이콘 로드"""
    try:
        icon_filename = CATEGORY_COLORS.get(category, CATEGORY_COLORS["기타"])["icon"]
        icon_path = os.path.join(ICONS_DIR, icon_filename)
        
        if os.path.exists(icon_path):
            icon = Image.open(icon_path)
            icon = icon.resize(size, Image.LANCZOS)
            return icon
        else:
            logger.warning(f"카테고리 아이콘 파일 없음: {icon_path}")
            return None
    except Exception as e:
        logger.warning(f"카테고리 아이콘 로드 실패: {e}")
        return None

def get_shipping_icon(shipping_type, size=(60, 60)):
    """배송 유형 아이콘 로드"""
    try:
        if shipping_type == "free":
            icon_path = FREE_SHIPPING_ICON
        elif shipping_type == "rocket":
            icon_path = ROCKET_SHIPPING_ICON
        else:
            return None
            
        if os.path.exists(icon_path):
            icon = Image.open(icon_path)
            icon = icon.resize(size, Image.LANCZOS)
            return icon
        else:
            return None
    except Exception as e:
        logger.warning(f"배송 아이콘 로드 실패: {e}")
        return None

def load_fonts():
    """폰트 로드"""
    try:
        fonts = {
            "title": ImageFont.truetype(FONT_SQUARE_BOLD, 60),
            "subtitle": ImageFont.truetype(FONT_SQUARE, 48),
            "product_title": ImageFont.truetype(FONT_BOLD, 48),
            "price": ImageFont.truetype(FONT_SQUARE_BOLD, 72),
            "original_price": ImageFont.truetype(FONT_REGULAR, 36),
            "discount": ImageFont.truetype(FONT_SQUARE_BOLD, 90),
            "discount_small": ImageFont.truetype(FONT_SQUARE_BOLD, 36),
            "regular": ImageFont.truetype(FONT_REGULAR, 36),
            "small": ImageFont.truetype(FONT_REGULAR, 24),
            "extra_small": ImageFont.truetype(FONT_REGULAR, 18)
        }
        return fonts
    except Exception as e:
        logger.error(f"폰트 로드 실패: {e}")
        # 기본 폰트 사용
        return {
            "title": ImageFont.load_default(),
            "subtitle": ImageFont.load_default(),
            "product_title": ImageFont.load_default(),
            "price": ImageFont.load_default(),
            "original_price": ImageFont.load_default(),
            "discount": ImageFont.load_default(),
            "discount_small": ImageFont.load_default(),
            "regular": ImageFont.load_default(),
            "small": ImageFont.load_default(),
            "extra_small": ImageFont.load_default()
        }

def create_rounded_rectangle(draw, xy, radius, fill=None, outline=None, width=1):
    """둥근 모서리 사각형 그리기"""
    x1, y1, x2, y2 = xy
    r = radius
    
    # 사각형 그리기
    draw.rectangle([(x1+r, y1), (x2-r, y2)], fill=fill, outline=outline, width=width)
    draw.rectangle([(x1, y1+r), (x2, y2-r)], fill=fill, outline=outline, width=width)
    
    # 네 모서리에 원 그리기
    draw.ellipse([(x1, y1), (x1+2*r, y1+2*r)], fill=fill, outline=outline, width=width)
    draw.ellipse([(x2-2*r, y1), (x2, y1+2*r)], fill=fill, outline=outline, width=width)
    draw.ellipse([(x1, y2-2*r), (x1+2*r, y2)], fill=fill, outline=outline, width=width)
    draw.ellipse([(x2-2*r, y2-2*r), (x2, y2)], fill=fill, outline=outline, width=width)

def add_shadow(image, offset=(10, 10), shadow_color="#00000080", iterations=5):
    """이미지에 그림자 효과 추가"""
    # RGBA 모드로 변환
    if image.mode != 'RGBA':
        image = image.convert('RGBA')
    
    # 그림자용 마스크 생성
    mask = Image.new('RGBA', image.size, (0, 0, 0, 0))
    shadow = Image.new('RGBA', image.size, shadow_color)
    
    # 원본 이미지의 알파 채널을 마스크로 사용
    mask.paste(shadow, offset, image)
    
    # 그림자 블러 처리
    for _ in range(iterations):
        mask = mask.filter(ImageFilter.BLUR)
    
    # 그림자와 원본 이미지 합성
    result = Image.new('RGBA', image.size, (0, 0, 0, 0))
    result.paste(mask, (0, 0))
    result.paste(image, (0, 0), image)
    
    return result

def create_discount_badge(discount, size=(200, 200), color=BRAND_COLORS["primary"]):
    """할인율 배지 생성"""
    # 원형 배지 생성
    badge = Image.new('RGBA', size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(badge)
    
    # 원 그리기
    center_x, center_y = size[0] // 2, size[1] // 2
    radius = min(center_x, center_y) - 5
    
    # 그라데이션 효과를 위한 여러 원 그리기
    for r in range(radius, radius-10, -1):
        # 색상 약간 변형
        r_val = int(color[1:3], 16)
        g_val = int(color[3:5], 16)
        b_val = int(color[5:7], 16)
        
        # 바깥쪽은 어둡게, 안쪽은 밝게
        factor = 1 - (radius - r) / 10 * 0.3
        r_val = min(255, int(r_val * factor))
        g_val = min(255, int(g_val * factor))
        b_val = min(255, int(b_val * factor))
        
        circle_color = f"#{r_val:02x}{g_val:02x}{b_val:02x}"
        draw.ellipse([(center_x-r, center_y-r), (center_x+r, center_y+r)], fill=circle_color)
    
    # 텍스트 추가
    font_discount = ImageFont.truetype(FONT_SQUARE_BOLD, size[0] // 3)
    font_percent = ImageFont.truetype(FONT_SQUARE_BOLD, size[0] // 5)
    
    # 할인율 텍스트
    discount_text = f"{discount}"
    percent_text = "%"
    
    # 텍스트 위치 계산
    discount_width = draw.textlength(discount_text, font=font_discount)
    percent_width = draw.textlength(percent_text, font=font_percent)
    
    # 텍스트 그리기
    draw.text((center_x - discount_width // 2, center_y - size[1] // 4), 
             discount_text, font=font_discount, fill=BRAND_COLORS["text_light"])
    draw.text((center_x - percent_width // 2, center_y + size[1] // 8), 
             percent_text, font=font_percent, fill=BRAND_COLORS["text_light"])
    
    # "SALE" 텍스트 추가
    sale_text = "SALE"
    sale_font = ImageFont.truetype(FONT_SQUARE_BOLD, size[0] // 6)
    sale_width = draw.textlength(sale_text, font=sale_font)
    draw.text((center_x - sale_width // 2, center_y - size[1] // 2 + 20), 
             sale_text, font=sale_font, fill=BRAND_COLORS["text_light"])
    
    return badge

def generate_qr_code(url, size=(150, 150)):
    """QR 코드 생성"""
    try:
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(url)
        qr.make(fit=True)
        
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img = qr_img.resize(size)
        
        # 둥근 모서리 효과 추가
        qr_img = qr_img.convert("RGBA")
        circle_mask = Image.new('L', qr_img.size, 0)
        circle_draw = ImageDraw.Draw(circle_mask)
        circle_draw.rounded_rectangle([(0, 0), qr_img.size], radius=15, fill=255)
        
        # 마스크 적용
        result = Image.new('RGBA', qr_img.size, (255, 255, 255, 0))
        result.paste(qr_img, (0, 0), circle_mask)
        
        return result
    except Exception as e:
        logger.warning(f"QR 코드 생성 실패: {e}")
        return None

def create_intro_frame(fonts, timestamp):
    """인트로 프레임 생성"""
    # 배경 이미지 로드
    background = get_background_image()
    draw = ImageDraw.Draw(background)
    
    # 반투명 오버레이 추가
    overlay = Image.new('RGBA', (FRAME_WIDTH, FRAME_HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    
    # 중앙 패널 (둥근 모서리 사각형)
    panel_width, panel_height = 1200, 600
    panel_x = (FRAME_WIDTH - panel_width) // 2
    panel_y = (FRAME_HEIGHT - panel_height) // 2
    
    # 그림자 효과 (약간 오프셋된 검은색 패널)
    shadow_offset = 15
    create_rounded_rectangle(overlay_draw, 
                           [panel_x + shadow_offset, panel_y + shadow_offset, 
                            panel_x + panel_width + shadow_offset, panel_y + panel_height + shadow_offset], 
                           radius=30, 
                           fill="#00000050")
    
    # 메인 패널
    create_rounded_rectangle(overlay_draw, 
                           [panel_x, panel_y, panel_x + panel_width, panel_y + panel_height], 
                           radius=30, 
                           fill="#FFFFFF")
    
    # 상단 장식 바
    bar_height = 10
    create_rounded_rectangle(overlay_draw, 
                           [panel_x, panel_y, panel_x + panel_width, panel_y + bar_height], 
                           radius=5, 
                           fill=BRAND_COLORS["primary"])
    
    # 배경에 오버레이 합성
    background = Image.alpha_composite(background.convert('RGBA'), overlay).convert('RGB')
    draw = ImageDraw.Draw(background)
    
    # 타이틀 텍스트
    title_text = "오늘의 쿠팡 핫딜"
    subtitle_text = datetime.now().strftime("%Y년 %m월 %d일")
    
    # 타이틀 위치 계산
    title_width = draw.textlength(title_text, font=fonts["title"])
    title_x = (FRAME_WIDTH - title_width) // 2
    
    # 서브타이틀 위치 계산
    subtitle_width = draw.textlength(subtitle_text, font=fonts["subtitle"])
    subtitle_x = (FRAME_WIDTH - subtitle_width) // 2
    
    # 텍스트 그리기
    draw.text((title_x, panel_y + 100), title_text, font=fonts["title"], fill=BRAND_COLORS["primary"])
    draw.text((subtitle_x, panel_y + 200), subtitle_text, font=fonts["subtitle"], fill=BRAND_COLORS["text_dark"])
    
    # 추가 텍스트
    info_text = "최대 90% 할인 상품 모음"
    info_width = draw.textlength(info_text, font=fonts["regular"])
    info_x = (FRAME_WIDTH - info_width) // 2
    draw.text((info_x, panel_y + 300), info_text, font=fonts["regular"], fill=BRAND_COLORS["text_dark"])
    
    # 로고 추가
    logo = get_logo_image((200, 200))
    if logo:
        # 로고에 그림자 효과 추가
        logo = add_shadow(logo)
        # 패널 상단 중앙에 로고 배치
        logo_x = panel_x + (panel_width - logo.width) // 2
        logo_y = panel_y - logo.height // 2  # 패널 상단에 걸치도록
        background.paste(logo, (logo_x, logo_y), logo if logo.mode == 'RGBA' else None)
    
    # 메타데이터 추가
    metadata = {
        "type": "intro",
        "timestamp": timestamp,
        "timecode": "00:00:00"
    }
    
    return background, metadata

def create_category_intro_frame(category, fonts, timestamp, frame_index):
    """카테고리 인트로 프레임 생성"""
    # 배경 이미지 로드
    background = get_background_image()
    
    # 카테고리 색상 가져오기
    colors = CATEGORY_COLORS.get(category, CATEGORY_COLORS["기타"])
    
    # 반투명 오버레이 생성
    overlay = Image.new('RGBA', (FRAME_WIDTH, FRAME_HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    
    # 상단 배너 (그라데이션 효과)
    banner_height = 200
    for y in range(banner_height):
        # 색상 그라데이션 계산
        progress = y / banner_height
        r1, g1, b1 = int(colors["primary"][1:3], 16), int(colors["primary"][3:5], 16), int(colors["primary"][5:7], 16)
        r2, g2, b2 = int(colors["secondary"][1:3], 16), int(colors["secondary"][3:5], 16), int(colors["secondary"][5:7], 16)
        
        r = int(r1 + (r2 - r1) * progress)
        g = int(g1 + (g2 - g1) * progress)
        b = int(b1 + (b2 - b1) * progress)
        
        alpha = 200  # 반투명
        
        overlay_draw.line([(0, 100 + y), (FRAME_WIDTH, 100 + y)], 
                         fill=(r, g, b, alpha))
    
    # 중앙 패널 (둥근 모서리 사각형)
    panel_width, panel_height = 1400, 500
    panel_x = (FRAME_WIDTH - panel_width) // 2
    panel_y = (FRAME_HEIGHT - panel_height) // 2 + 50
    
    # 그림자 효과
    shadow_offset = 15
    create_rounded_rectangle(overlay_draw, 
                           [panel_x + shadow_offset, panel_y + shadow_offset, 
                            panel_x + panel_width + shadow_offset, panel_y + panel_height + shadow_offset], 
                           radius=30, 
                           fill="#00000050")
    
    # 메인 패널
    create_rounded_rectangle(overlay_draw, 
                           [panel_x, panel_y, panel_x + panel_width, panel_y + panel_height], 
                           radius=30, 
                           fill="#FFFFFF")
    
    # 배경에 오버레이 합성
    background = Image.alpha_composite(background.convert('RGBA'), overlay).convert('RGB')
    draw = ImageDraw.Draw(background)
    
    # 카테고리 텍스트
    category_text = f"{category} 핫딜"
    text_width = draw.textlength(category_text, font=fonts["title"])
    text_x = (FRAME_WIDTH - text_width) // 2
    
    # 텍스트 그리기 (그림자 효과 추가)
    # 그림자
    draw.text((text_x + 3, 153), category_text, font=fonts["title"], fill="#00000050")
    # 메인 텍스트
    draw.text((text_x, 150), category_text, font=fonts["title"], fill=BRAND_COLORS["text_light"])
    
    # 설명 텍스트
    description = f"오늘의 {category} 핫딜 상품을 소개합니다"
    desc_width = draw.textlength(description, font=fonts["subtitle"])
    desc_x = (FRAME_WIDTH - desc_width) // 2
    draw.text((desc_x, panel_y + 100), description, font=fonts["subtitle"], fill=BRAND_COLORS["text_dark"])
    
    # 추가 정보
    info_text = "놓치면 후회할 특가 상품들"
    info_width = draw.textlength(info_text, font=fonts["regular"])
    info_x = (FRAME_WIDTH - info_width) // 2
    draw.text((info_x, panel_y + 200), info_text, font=fonts["regular"], fill=BRAND_COLORS["text_dark"])
    
    # 날짜 정보
    date_text = datetime.now().strftime("%Y년 %m월 %d일")
    date_width = draw.textlength(date_text, font=fonts["regular"])
    date_x = (FRAME_WIDTH - date_width) // 2
    draw.text((date_x, panel_y + 300), date_text, font=fonts["regular"], fill=BRAND_COLORS["text_dark"])
    
    # 카테고리 아이콘 추가
    icon = get_category_icon(category, (150, 150))
    if icon:
        # 아이콘에 그림자 효과 추가
        icon = add_shadow(icon)
        # 패널 상단 중앙에 아이콘 배치
        icon_x = panel_x + (panel_width - icon.width) // 2
        icon_y = panel_y - icon.height // 2  # 패널 상단에 걸치도록
        background.paste(icon, (icon_x, icon_y), icon if icon.mode == 'RGBA' else None)
    
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
    overlay_draw.rectangle([(0, 50), (FRAME_WIDTH, 150)], fill=(255, 255, 255, 200))
    
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
        badge = create_discount_badge(discount, (badge_size, badge_size), colors["secondary"])
        background.paste(badge, (FRAME_WIDTH - 300 + (badge_size - badge.width) // 2, 250), badge if badge.mode == 'RGBA' else None)
        
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
    logo = get_logo_image((200, 200))
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