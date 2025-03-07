#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import glob
import time
import logging
import datetime
import tempfile
import numpy as np
from pathlib import Path

# MoviePy 기본 모듈들을 직접 임포트
import moviepy.video.io.ImageSequenceClip as ImageSequenceClip
from moviepy.video.VideoClip import ImageClip, TextClip
from moviepy.video.compositing.CompositeVideoClip import CompositeVideoClip
from moviepy.audio.AudioClip import AudioClip, CompositeAudioClip
from moviepy.audio.io.AudioFileClip import AudioFileClip
from moviepy.video.io.VideoFileClip import VideoFileClip
from moviepy.video.compositing.concatenate import concatenate_videoclips
from moviepy.video.fx.fadein import fadein
from moviepy.video.fx.fadeout import fadeout

# MoviePy 탐색을 위한 디버깅 코드 추가
import moviepy
print("MoviePy 경로:", moviepy.__file__)
print("MoviePy 버전:", moviepy.__version__)
print("MoviePy 구조:", dir(moviepy))

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("video_renderer.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("VideoRenderer")

# 상수 정의
DEFAULT_FRAME_DURATION = 5  # 기본 프레임 지속 시간 (초)
FADE_DURATION = 0.5  # 페이드 효과 지속 시간 (초)
DEFAULT_RESOLUTION = (1920, 1080)  # 기본 해상도
DEFAULT_FPS = 30  # 기본 FPS
DEFAULT_BITRATE = "8000k"  # 기본 비트레이트
DEFAULT_BG_MUSIC_VOLUME = 0.1  # 기본 배경 음악 볼륨
TEMP_DIR = tempfile.gettempdir()  # 임시 파일 디렉토리

def get_latest_timestamp(directory, pattern):
    """가장 최근 타임스탬프 가져오기"""
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    
    # 파일명에서 타임스탬프 추출 (예: 20230101_120000_00_intro.jpg)
    timestamps = set()
    for file in files:
        filename = os.path.basename(file)
        parts = filename.split('_')
        if len(parts) >= 2:
            timestamps.add(f"{parts[0]}_{parts[1]}")
    
    if not timestamps:
        return None
    
    return max(timestamps)

def load_frames_metadata(frames_dir, timestamp):
    """프레임 메타데이터 로드"""
    metadata_file = os.path.join(frames_dir, "metadata", f"{timestamp}_frames_info.json")
    
    if not os.path.exists(metadata_file):
        logger.error(f"메타데이터 파일을 찾을 수 없습니다: {metadata_file}")
        return None
    
    try:
        with open(metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"메타데이터 파일 로드 실패: {e}")
        return None

def find_audio_files(audio_dir, timestamp):
    """타임스탬프에 해당하는 오디오 파일 찾기"""
    pattern = os.path.join(audio_dir, f"{timestamp}_*.mp3")
    files = glob.glob(pattern)
    
    # 파일명 기준으로 정렬 (00_intro, 01_category, ...)
    return sorted(files, key=lambda x: os.path.basename(x).split('_')[2:])

def get_audio_duration(audio_file):
    """오디오 파일 길이 가져오기"""
    try:
        with AudioFileClip(audio_file) as audio:
            return audio.duration
    except Exception as e:
        logger.warning(f"오디오 파일 길이 확인 실패: {audio_file}, 오류: {e}")
        return DEFAULT_FRAME_DURATION

def create_subtitle(text, duration, font_size=36, font='Arial', color='white', bg_color='black', opacity=0.6):
    """자막 클립 생성"""
    try:
        # 텍스트가 너무 길면 줄바꿈
        if len(text) > 40:
            words = text.split()
            lines = []
            current_line = []
            
            for word in words:
                current_line.append(word)
                if len(' '.join(current_line)) > 40:
                    current_line.pop()
                    lines.append(' '.join(current_line))
                    current_line = [word]
            
            if current_line:
                lines.append(' '.join(current_line))
            
            text = '\n'.join(lines)
        
        txt_clip = TextClip(text, fontsize=font_size, font=font, color=color, bg_color=None)
        txt_clip = txt_clip.set_duration(duration)
        
        # 배경 생성
        bg_width = txt_clip.w + 20
        bg_height = txt_clip.h + 10
        bg_clip = TextClip(' ', fontsize=font_size, font=font, color=color, 
                          bg_color=bg_color, size=(bg_width, bg_height))
        bg_clip = bg_clip.set_duration(duration).set_opacity(opacity)
        
        # 자막과 배경 합성
        composite = CompositeVideoClip([
            bg_clip,
            txt_clip.set_position('center')
        ])
        
        return composite.set_position(('center', 'bottom'))
    except Exception as e:
        logger.error(f"자막 생성 실패: {e}")
        return None

def create_video_clip(frame_path, audio_path, add_subtitle=True, fade=True):
    """프레임과 오디오로 비디오 클립 생성"""
    try:
        # 오디오 길이 확인
        audio_duration = get_audio_duration(audio_path)
        
        # 이미지 클립 생성
        img_clip = ImageClip(frame_path).set_duration(audio_duration)
        
        # 오디오 클립 생성
        audio_clip = AudioFileClip(audio_path)
        img_clip = img_clip.set_audio(audio_clip)
        
        # 페이드 효과 적용
        if fade and audio_duration > FADE_DURATION * 2:
            img_clip = fadein(img_clip, FADE_DURATION)
            img_clip = fadeout(img_clip, FADE_DURATION)
        
        # 자막 추가
        if add_subtitle:
            # 프레임 메타데이터에서 텍스트 추출 (파일명에서 카테고리 정보 추출)
            filename = os.path.basename(frame_path)
            parts = filename.split('_')
            
            subtitle_text = ""
            if "category" in filename:
                category = parts[3] if len(parts) > 3 else "카테고리"
                subtitle_text = f"{category} 핫딜"
            elif "product" in filename:
                category = parts[3] if len(parts) > 3 else "상품"
                subtitle_text = f"{category} - 상품 {parts[2]}"
            elif "intro" in filename:
                subtitle_text = "오늘의 핫딜 소개"
            elif "outro" in filename:
                subtitle_text = "이상 오늘의 핫딜 정보였습니다"
            
            if subtitle_text:
                subtitle = create_subtitle(subtitle_text, audio_duration)
                if subtitle:
                    img_clip = CompositeVideoClip([img_clip, subtitle])
        
        return img_clip
    except Exception as e:
        logger.error(f"비디오 클립 생성 실패: {frame_path}, {audio_path}, 오류: {e}")
        # 오류 발생 시 기본 클립 반환
        return ImageClip(frame_path).set_duration(DEFAULT_FRAME_DURATION)

def add_background_music(video_clip, bg_music_path, volume=DEFAULT_BG_MUSIC_VOLUME):
    """배경 음악 추가"""
    if not os.path.exists(bg_music_path):
        logger.warning(f"배경 음악 파일을 찾을 수 없습니다: {bg_music_path}")
        return video_clip
    
    try:
        # 배경 음악 로드
        bg_music = AudioFileClip(bg_music_path)
        
        # 비디오 길이에 맞게 배경 음악 반복
        if bg_music.duration < video_clip.duration:
            repeats = int(np.ceil(video_clip.duration / bg_music.duration))
            bg_music = concatenate_audioclips([bg_music] * repeats)
        
        # 비디오 길이에 맞게 배경 음악 자르기
        bg_music = bg_music.subclip(0, video_clip.duration)
        
        # 볼륨 조절
        bg_music = bg_music.volumex(volume)
        
        # 기존 오디오와 배경 음악 합성
        if video_clip.audio:
            new_audio = CompositeAudioClip([video_clip.audio, bg_music])
            return video_clip.set_audio(new_audio)
        else:
            return video_clip.set_audio(bg_music)
    except Exception as e:
        logger.error(f"배경 음악 추가 실패: {e}")
        return video_clip

def add_watermark(video_clip, watermark_path, position=('right', 'bottom'), opacity=0.7):
    """워터마크 추가"""
    if not os.path.exists(watermark_path):
        logger.warning(f"워터마크 파일을 찾을 수 없습니다: {watermark_path}")
        return video_clip
    
    try:
        # 워터마크 이미지 로드
        watermark = ImageClip(watermark_path)
        
        # 워터마크 크기 조정 (비디오 너비의 10%)
        new_width = int(video_clip.w * 0.1)
        watermark = watermark.resize(width=new_width)
        
        # 투명도 설정
        watermark = watermark.set_opacity(opacity)
        
        # 워터마크 위치 및 지속 시간 설정
        watermark = watermark.set_position(position).set_duration(video_clip.duration)
        
        # 워터마크 합성
        return CompositeVideoClip([video_clip, watermark])
    except Exception as e:
        logger.error(f"워터마크 추가 실패: {e}")
        return video_clip

def render_video(frames_info, audio_files, output_path, bg_music_path=None, watermark_path=None, 
                resolution=DEFAULT_RESOLUTION, fps=DEFAULT_FPS, bitrate=DEFAULT_BITRATE, 
                add_subtitles=True, add_transitions=True):
    """최종 비디오 렌더링"""
    if not frames_info or not audio_files:
        logger.error("프레임 정보 또는 오디오 파일이 없습니다.")
        return False
    
    # 프레임 수와 오디오 파일 수 확인
    if len(frames_info) != len(audio_files):
        logger.warning(f"프레임 수({len(frames_info)})와 오디오 파일 수({len(audio_files)})가 일치하지 않습니다.")
    
    try:
        clips = []
        
        # 각 프레임과 오디오 파일로 클립 생성
        for i, (frame_info, audio_file) in enumerate(zip(frames_info, audio_files)):
            logger.info(f"클립 생성 중 ({i+1}/{len(frames_info)}): {os.path.basename(frame_info['filename'])}")
            
            clip = create_video_clip(
                frame_info['filename'], 
                audio_file, 
                add_subtitle=add_subtitles,
                fade=add_transitions
            )
            
            clips.append(clip)
        
        # 모든 클립 연결
        logger.info("클립 연결 중...")
        final_clip = concatenate_videoclips(clips, method="compose")
        
        # 배경 음악 추가
        if bg_music_path:
            logger.info("배경 음악 추가 중...")
            final_clip = add_background_music(final_clip, bg_music_path)
        
        # 워터마크 추가
        if watermark_path:
            logger.info("워터마크 추가 중...")
            final_clip = add_watermark(final_clip, watermark_path)
        
        # 출력 디렉토리 생성
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 비디오 렌더링
        logger.info(f"비디오 렌더링 중: {output_path}")
        temp_output = os.path.join(TEMP_DIR, f"temp_output_{int(time.time())}.mp4")
        
        final_clip.write_videofile(
            temp_output,
            fps=fps,
            codec='libx264',
            audio_codec='aac',
            bitrate=bitrate,
            threads=4,
            preset='medium',
            logger=None  # moviepy 내부 로거 비활성화
        )
        
        # 임시 파일을 최종 위치로 이동
        import shutil
        shutil.move(temp_output, output_path)
        
        logger.info(f"비디오 렌더링 완료: {output_path}")
        return True
    
    except Exception as e:
        logger.error(f"비디오 렌더링 실패: {e}", exc_info=True)
        return False
    finally:
        # 메모리 정리
        try:
            for clip in clips:
                clip.close()
            final_clip.close()
        except:
            pass

def main():
    try:
        # 환경 변수에서 설정 가져오기 (기본값 제공)
        frames_dir = os.environ.get('FRAMES_DIR', 'frames')
        audio_dir = os.environ.get('AUDIO_DIR', 'audio')
        output_dir = os.environ.get('OUTPUT_DIR', 'output')
        bg_music_path = os.environ.get('BG_MUSIC_PATH', os.path.join('assets', 'music', 'background.mp3'))
        watermark_path = os.environ.get('WATERMARK_PATH', os.path.join('assets', 'logo', 'watermark.png'))
        resolution = tuple(map(int, os.environ.get('RESOLUTION', '1920,1080').split(',')))
        fps = int(os.environ.get('FPS', DEFAULT_FPS))
        bitrate = os.environ.get('BITRATE', DEFAULT_BITRATE)
        add_subtitles = os.environ.get('ADD_SUBTITLES', 'True').lower() == 'true'
        add_transitions = os.environ.get('ADD_TRANSITIONS', 'True').lower() == 'true'
        max_retries = int(os.environ.get('MAX_RETRIES', 3))
        
        # 최신 타임스탬프 찾기
        frames_timestamp = get_latest_timestamp(frames_dir, "*.jpg")
        audio_timestamp = get_latest_timestamp(audio_dir, "*.mp3")
        
        if not frames_timestamp or not audio_timestamp:
            logger.error("프레임 또는 오디오 파일을 찾을 수 없습니다.")
            return
        
        logger.info(f"프레임 타임스탬프: {frames_timestamp}, 오디오 타임스탬프: {audio_timestamp}")
        
        # 타임스탬프가 다른 경우 경고
        if frames_timestamp != audio_timestamp:
            logger.warning(f"프레임과 오디오 타임스탬프가 일치하지 않습니다: {frames_timestamp} != {audio_timestamp}")
            # 더 최근 타임스탬프 사용
            timestamp = max(frames_timestamp, audio_timestamp)
        else:
            timestamp = frames_timestamp
        
        # 프레임 메타데이터 로드
        frames_info = load_frames_metadata(frames_dir, timestamp)
        if not frames_info:
            logger.error("프레임 메타데이터를 로드할 수 없습니다.")
            return
        
        # 오디오 파일 찾기
        audio_files = find_audio_files(audio_dir, timestamp)
        if not audio_files:
            logger.error("오디오 파일을 찾을 수 없습니다.")
            return
        
        logger.info(f"프레임 수: {len(frames_info)}, 오디오 파일 수: {len(audio_files)}")
        
        # 출력 파일 경로
        output_path = os.path.join(output_dir, f"hotdeal_video_{timestamp}.mp4")
        
        # 비디오 렌더링 (재시도 로직 포함)
        success = False
        for attempt in range(max_retries):
            try:
                logger.info(f"비디오 렌더링 시도 {attempt+1}/{max_retries}")
                success = render_video(
                    frames_info, 
                    audio_files, 
                    output_path,
                    bg_music_path=bg_music_path,
                    watermark_path=watermark_path,
                    resolution=resolution,
                    fps=fps,
                    bitrate=bitrate,
                    add_subtitles=add_subtitles,
                    add_transitions=add_transitions
                )
                
                if success:
                    logger.info(f"비디오 렌더링 성공: {output_path}")
                    break
            except Exception as e:
                logger.error(f"렌더링 시도 {attempt+1} 실패: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"5초 후 재시도합니다...")
                    time.sleep(5)
        
        if not success:
            logger.error(f"최대 재시도 횟수 초과. 비디오 렌더링 실패")
        
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main() 