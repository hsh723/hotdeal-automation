#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging
import argparse
import datetime
import subprocess
import traceback
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("hotdeal_pipeline.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HotdealPipeline")

# 상수 정의
DEFAULT_TIMEOUT = 3600  # 각 단계 기본 타임아웃 (초)
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 60  # 재시도 간격 (초)
PIPELINE_STEPS = [
    "crawler",
    "telegram_sender",
    "script_generator",
    "tts_generator",
    "video_frame_generator",
    "video_renderer",
    "youtube_uploader"
]
RESULTS_FILE = "pipeline_results.json"

def run_step(step_name, timeout=DEFAULT_TIMEOUT, env=None):
    """파이프라인 단계 실행"""
    script_path = f"{step_name}.py"
    
    if not os.path.exists(script_path):
        logger.error(f"스크립트 파일을 찾을 수 없습니다: {script_path}")
        return False
    
    logger.info(f"===== {step_name} 단계 시작 =====")
    start_time = time.time()
    
    # 환경 변수 설정
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    
    # 실행 명령
    cmd = [sys.executable, script_path]
    
    try:
        # 서브프로세스 실행
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=process_env
        )
        
        # 타임아웃 처리
        stdout, stderr = "", ""
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            logger.error(f"{step_name} 단계가 타임아웃되었습니다 ({timeout}초)")
            return False
        
        # 결과 확인
        if process.returncode != 0:
            logger.error(f"{step_name} 단계가 실패했습니다 (코드: {process.returncode})")
            logger.error(f"오류 메시지: {stderr}")
            return False
        
        # 로그 출력
        for line in stdout.splitlines():
            logger.info(f"[{step_name}] {line}")
        
        execution_time = time.time() - start_time
        logger.info(f"===== {step_name} 단계 완료 (소요 시간: {execution_time:.2f}초) =====")
        return True
    
    except Exception as e:
        logger.error(f"{step_name} 단계 실행 중 오류 발생: {e}")
        logger.error(traceback.format_exc())
        return False

def run_pipeline(steps=None, start_from=None, end_at=None, retry=True):
    """전체 파이프라인 실행"""
    if steps is None:
        # 시작 및 종료 단계 처리
        if start_from:
            start_index = PIPELINE_STEPS.index(start_from) if start_from in PIPELINE_STEPS else 0
        else:
            start_index = 0
            
        if end_at:
            end_index = PIPELINE_STEPS.index(end_at) + 1 if end_at in PIPELINE_STEPS else len(PIPELINE_STEPS)
        else:
            end_index = len(PIPELINE_STEPS)
            
        steps = PIPELINE_STEPS[start_index:end_index]
    
    logger.info(f"파이프라인 실행 시작: {', '.join(steps)}")
    
    results = {
        "start_time": datetime.datetime.now().isoformat(),
        "steps": {}
    }
    
    success = True
    for step in steps:
        step_start_time = time.time()
        step_success = False
        retry_count = 0
        
        # 재시도 로직
        while not step_success and retry_count <= MAX_RETRIES:
            if retry_count > 0:
                logger.info(f"{step} 단계 재시도 ({retry_count}/{MAX_RETRIES})...")
                time.sleep(RETRY_DELAY)
            
            step_success = run_step(step)
            
            if not step_success and retry and retry_count < MAX_RETRIES:
                retry_count += 1
            else:
                break
        
        # 결과 기록
        step_end_time = time.time()
        results["steps"][step] = {
            "success": step_success,
            "start_time": datetime.datetime.fromtimestamp(step_start_time).isoformat(),
            "end_time": datetime.datetime.fromtimestamp(step_end_time).isoformat(),
            "duration_seconds": step_end_time - step_start_time,
            "retries": retry_count
        }
        
        # 실패 시 파이프라인 중단
        if not step_success:
            logger.error(f"{step} 단계가 실패하여 파이프라인을 중단합니다.")
            success = False
            break
    
    # 최종 결과 기록
    results["end_time"] = datetime.datetime.now().isoformat()
    results["success"] = success
    
    # 결과 파일 저장
    save_results(results)
    
    return success, results

def save_results(results):
    """파이프라인 실행 결과 저장"""
    # 기존 결과 파일이 있으면 로드
    history = []
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception as e:
            logger.warning(f"결과 파일 로드 실패: {e}")
    
    # 새 결과 추가
    history.append(results)
    
    # 결과 파일 저장
    try:
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        logger.info(f"파이프라인 결과 저장 완료: {RESULTS_FILE}")
    except Exception as e:
        logger.error(f"결과 파일 저장 실패: {e}")

def schedule_pipeline(schedule_time=None, steps=None, start_from=None, end_at=None):
    """파이프라인 예약 실행"""
    if schedule_time is None:
        # 기본값: 다음 날 오전 9시
        now = datetime.datetime.now()
        schedule_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if schedule_time <= now:
            schedule_time += datetime.timedelta(days=1)
    
    wait_seconds = (schedule_time - datetime.datetime.now()).total_seconds()
    
    if wait_seconds <= 0:
        logger.warning("예약 시간이 현재 시간보다 이전입니다. 즉시 실행합니다.")
    else:
        logger.info(f"파이프라인 예약 완료: {schedule_time.isoformat()} (대기 시간: {wait_seconds:.2f}초)")
        time.sleep(wait_seconds)
    
    return run_pipeline(steps, start_from, end_at)

def parse_args():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(description='핫딜 자동화 파이프라인')
    
    # 실행 모드 설정
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--run', action='store_true', help='즉시 실행')
    mode_group.add_argument('--schedule', type=str, help='예약 실행 (형식: YYYY-MM-DD HH:MM)')
    
    # 단계 설정 (상호 배타적 그룹에서 제거)
    parser.add_argument('--steps', type=str, nargs='+', choices=PIPELINE_STEPS, 
                      help='실행할 특정 단계 (공백으로 구분)')
    parser.add_argument('--start-from', type=str, choices=PIPELINE_STEPS,
                      help='시작할 단계')
    parser.add_argument('--end-at', type=str, choices=PIPELINE_STEPS,
                      help='종료할 단계')
    
    # 기타 옵션
    parser.add_argument('--no-retry', action='store_true', help='실패 시 재시도하지 않음')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT, 
                      help=f'각 단계 타임아웃 (초, 기본값: {DEFAULT_TIMEOUT})')
    
    return parser.parse_args()

def main():
    """메인 함수"""
    args = parse_args()
    
    try:
        # 환경 변수 설정
        os.environ['PIPELINE_TIMEOUT'] = str(args.timeout)
        
        # 실행할 단계 결정
        steps = args.steps
        start_from = args.start_from
        end_at = args.end_at
        
        # 실행 모드에 따라 처리
        if args.schedule:
            try:
                schedule_time = datetime.datetime.strptime(args.schedule, "%Y-%m-%d %H:%M")
                logger.info(f"파이프라인 예약 실행: {schedule_time}")
                success, results = schedule_pipeline(schedule_time, steps, start_from, end_at)
            except ValueError:
                logger.error(f"잘못된 날짜/시간 형식: {args.schedule} (올바른 형식: YYYY-MM-DD HH:MM)")
                return 1
        else:
            # 즉시 실행
            logger.info("파이프라인 즉시 실행")
            success, results = run_pipeline(steps, start_from, end_at, not args.no_retry)
        
        # 결과 출력
        if success:
            logger.info("파이프라인 실행 성공!")
            return 0
        else:
            logger.error("파이프라인 실행 실패")
            return 1
    
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 파이프라인이 중단되었습니다.")
        return 130
    except Exception as e:
        logger.error(f"파이프라인 실행 중 예상치 못한 오류 발생: {e}")
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 