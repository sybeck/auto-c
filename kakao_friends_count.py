import re
import time
from datetime import datetime
from typing import Optional, List, Tuple, Dict
import os
import json
import requests

import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from dotenv import load_dotenv
load_dotenv()

# =======================
# ✅ 설정 (여기만 수정)
# =======================
SERVICE_ACCOUNT_JSON = "service_account.json"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1J0muYgf29eqIMDe1BmYKTtS5-tP1KcV2M5ojv1WRHNw/edit?gid=0#gid=0"
WORKSHEET_NAME = "시트1"

NAME_ROW = 1            # 1행: 이름(브랜드명/채널명) - 짝수열에 이름이 있다고 가정
HEADER_ROW = 2          # 2행: 카카오 채널 ID (짝수열만)
DATA_START_ROW = 3      # A3부터 날짜/데이터
DATE_COL = 1            # A열: 날짜

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

SLEEP_BETWEEN = 1.0     # URL 1개마다 1초 지연
DATE_FORMAT = "%Y-%m-%d"

# 재시도 설정
RETRY_DELAY = 2.0        # 실패 시 재시도 간격(초)
MAX_RETRY_TIME = 120.0   # 한 채널당 최대 대기 시간(초)

# 랭킹/알림 설정
TOP_N = 5
DELTA_CHANGE_THRESHOLD = 0.30  # 30%


# =======================
# ✅ 유틸
# =======================
def normalize_korean_number(text: str) -> Optional[int]:
    text = (text or "").strip().replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*만", text)
    if m:
        return int(float(m.group(1)) * 10000)
    m2 = re.search(r"(\d+)", text)
    return int(m2.group(1)) if m2 else None


def fmt(n: int) -> str:
    return f"{n:,}"


def safe_int(cell_value) -> Optional[int]:
    if cell_value is None:
        return None
    s = str(cell_value).strip().replace(",", "")
    if s == "":
        return None
    try:
        return int(float(s))
    except:
        return None


def row_values_1based(ws, row: int) -> List[Optional[str]]:
    """
    ws.row_values(row)는 값이 있는 데까지만 오므로,
    1-based 인덱스(A=1)에 안전하게 접근할 수 있게 앞에 더미를 붙인다.
    """
    vals = ws.row_values(row)
    return [None] + vals


def get_cell_from_row(row_1based: List[Optional[str]], col: int) -> Optional[str]:
    if col < len(row_1based):
        return row_1based[col]
    return None


def delta_change_ratio(prev_delta: int, today_delta: int) -> float:
    """
    전날 증감량 대비 오늘 증감량 변화율(절대 기준)
    prev_delta == 0:
      - today_delta == 0 -> 0
      - today_delta != 0 -> inf
    """
    if prev_delta == 0:
        return float("inf") if today_delta != 0 else 0.0
    return abs(today_delta - prev_delta) / abs(prev_delta)


# =======================
# ✅ Slack
# =======================
def send_to_slack(message: str):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL 환경변수가 없어 Slack 전송 생략")
        return

    payload = {"text": message}

    try:
        r = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if r.status_code != 200:
            print(f"[WARN] Slack 전송 실패: {r.status_code} {r.text}")
    except Exception as e:
        print(f"[WARN] Slack 전송 중 예외 발생: {e}")


# =======================
# ✅ 크롤링
# =======================
def extract_friend_count_from_html(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    patterns = [
        r"친구\s*수?\s*[:：]?\s*([0-9,]+(?:\.\d+)?\s*만?)",
        r"친구\s*([0-9,]+(?:\.\d+)?\s*만?)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return normalize_korean_number(m.group(1))
    return None


def get_friend_count_playwright(page, kakao_id: str) -> Optional[int]:
    url = f"https://pf.kakao.com/{kakao_id}"
    page.goto(url, wait_until="networkidle", timeout=30000)
    html = page.content()
    return extract_friend_count_from_html(html)


def get_friend_count_with_retry(page, kakao_id: str) -> int:
    start_time = time.time()
    attempt = 0

    while True:
        attempt += 1
        cnt = get_friend_count_playwright(page, kakao_id)
        if cnt is not None:
            return cnt

        elapsed = time.time() - start_time
        print(f"[RETRY] {kakao_id} attempt {attempt} 실패, {RETRY_DELAY}s 후 재시도")

        if elapsed >= MAX_RETRY_TIME:
            raise TimeoutError(f"{kakao_id} 친구수 조회 실패: {MAX_RETRY_TIME}s 초과")

        time.sleep(RETRY_DELAY)


# =======================
# ✅ Sheets
# =======================
def connect_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(GOOGLE_SHEET_URL)
    return sh.worksheet(WORKSHEET_NAME)


def get_targets_from_header(ws) -> List[Tuple[int, str]]:
    """
    2행에서 짝수열(B,D,F,...)에 있는 카카오 ID만 타겟
    반환: [(friend_col, kakao_id), ...]
    """
    header = row_values_1based(ws, HEADER_ROW)
    results: List[Tuple[int, str]] = []

    # col=2부터 짝수만
    for col in range(2, len(header), 2):
        v = (header[col] or "").strip()
        if v:
            results.append((col, v))
    return results


def find_next_empty_row_and_prev_row(ws) -> Tuple[int, Optional[int]]:
    """
    A열(날짜) 기준으로 다음 빈 행 + 바로 이전(마지막 기록) 행을 계산.
    읽기 요청 최소화를 위해 col_values 1회만 사용.
    """
    colA = ws.col_values(DATE_COL)  # 값이 있는 만큼만 옴
    # colA는 1행부터 시작. DATA_START_ROW부터 검사.
    # 중간에 빈칸이 없다는 전제(로그 기록형)에서 가장 안정적이고 빠름.

    # 현재 입력된 마지막 행 번호:
    last_filled_row = len(colA)

    # DATA_START_ROW 이전만 있고 데이터가 없으면
    if last_filled_row < DATA_START_ROW:
        target_row = DATA_START_ROW
        prev_row = None
        return target_row, prev_row

    # A열이 연속으로 채워지는 구조면,
    # 다음 빈 행은 last_filled_row + 1
    target_row = last_filled_row + 1

    # prev_row는 last_filled_row가 DATA_START_ROW 이상일 때
    prev_row = last_filled_row if last_filled_row >= DATA_START_ROW else None
    return target_row, prev_row


# =======================
# ✅ main
# =======================
def main():
    ws = connect_sheet()

    # ✅ 필요한 행을 한 번에 읽어 쿼터 절약
    name_row = row_values_1based(ws, NAME_ROW)

    targets = get_targets_from_header(ws)
    if not targets:
        raise RuntimeError("2행(HEADER_ROW) 짝수열에 트래킹할 ID가 없습니다. (B2, D2, F2...)")

    target_row, prev_row = find_next_empty_row_and_prev_row(ws)
    today_str = datetime.now().strftime(DATE_FORMAT)

    print(f"[INFO] 기록 행: {target_row}, 날짜: {today_str}")
    print(f"[INFO] 이전 비교 행: {prev_row if prev_row else '없음(첫 기록)'}")
    print(f"[INFO] 대상 수: {len(targets)}")

    # ✅ 이름 맵(짝수열 기준)
    name_map: Dict[int, str] = {}
    for friend_col, _ in targets:
        nm = get_cell_from_row(name_row, friend_col)
        nm = (nm or "").strip()
        name_map[friend_col] = nm if nm else f"(col {friend_col})"

    # ✅ prev_row 값은 한 번만 읽기 (429 방지 핵심)
    prev_row_vals = row_values_1based(ws, prev_row) if prev_row else [None]

    # 1) Playwright로 친구수 수집
    current_counts: Dict[int, int] = {}
    base_updates: List[gspread.Cell] = [gspread.Cell(target_row, DATE_COL, today_str)]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=USER_AGENT)

        for friend_col, kakao_id in targets:
            try:
                cnt = get_friend_count_with_retry(page, kakao_id)
                print(f"- {name_map[friend_col]} / {kakao_id} -> {cnt}")
                current_counts[friend_col] = cnt
                base_updates.append(gspread.Cell(target_row, friend_col, cnt))
                time.sleep(SLEEP_BETWEEN)
            except Exception as e:
                print(f"[ERROR] {name_map[friend_col]} / {kakao_id} (col {friend_col}): {e}")

        browser.close()

    # ✅ 날짜 + 친구수 기록(한 번에)
    ws.update_cells(base_updates, value_input_option="USER_ENTERED")
    print(f"[INFO] 저장 완료: 날짜 1개 + 친구수 {len(base_updates)-1}개")

    # 이전 행 없으면 여기서 종료
    if not prev_row:
        print("[RANK] 이전 행이 없어 증가량/증가율/증감량 변화 계산을 건너뜁니다.")
        return

    # 2) 증가량/증가율 계산 + 증감량을 (friend_col+1) 홀수열에 기록
    deltas = []  # (delta, name, friend_col, prev_friend, curr_friend)
    rates = []   # (rate, name, friend_col, prev_friend, curr_friend, delta)
    delta_updates: List[gspread.Cell] = []

    # 3) 증감량 변화 30% 이상 탐지용
    delta_change_hits = []  # (ratio, name, prev_delta, today_delta)

    for friend_col, _kakao_id in targets:
        curr = current_counts.get(friend_col)
        if curr is None:
            continue

        prev_friend = safe_int(get_cell_from_row(prev_row_vals, friend_col))
        if prev_friend is None:
            continue

        delta = curr - prev_friend
        name = name_map[friend_col]

        deltas.append((delta, name, friend_col, prev_friend, curr))
        if prev_friend > 0:
            rates.append((delta / prev_friend, name, friend_col, prev_friend, curr, delta))

        # ✅ 증감량 열 규칙: "친구수 열 + 1" (2열 -> 3열, 4열 -> 5열 ...)
        delta_col = friend_col + 1
        delta_updates.append(gspread.Cell(target_row, delta_col, delta))

        # ✅ 전날 증감량은 prev_row의 delta_col에서 읽음(행 1번 읽어둔 값에서 꺼냄)
        prev_delta = safe_int(get_cell_from_row(prev_row_vals, delta_col))
        if prev_delta is not None:
            ratio = delta_change_ratio(prev_delta, delta)
            if ratio >= DELTA_CHANGE_THRESHOLD:
                delta_change_hits.append((ratio, name, prev_delta, delta))

    # ✅ 오늘 증감량 기록(한 번에)
    if delta_updates:
        ws.update_cells(delta_updates, value_input_option="USER_ENTERED")

    # TOP 5
    deltas.sort(key=lambda x: x[0], reverse=True)
    rates.sort(key=lambda x: x[0], reverse=True)

    top_deltas = deltas[:TOP_N]
    top_rates = rates[:TOP_N]

    print(f"\n========== [TOP {TOP_N}] 증가량(Δ) ==========")
    for i, (delta, name, _friend_col, prev_friend, curr) in enumerate(top_deltas, start=1):
        sign = "+" if delta >= 0 else ""
        print(f"{i:02d}. {name}  {fmt(prev_friend)} → {fmt(curr)}  (Δ {sign}{fmt(delta)})")

    print(f"\n========== [TOP {TOP_N}] 증가율(Δ/이전) ==========")
    for i, (rate, name, _friend_col, prev_friend, curr, delta) in enumerate(top_rates, start=1):
        sign = "+" if delta >= 0 else ""
        print(f"{i:02d}. {name}  {fmt(prev_friend)} → {fmt(curr)}  (Δ {sign}{fmt(delta)}, {rate*100:.2f}%)")

    # 증감량 변화 30% 이상 (ratio 큰 순, inf 최상단)
    def sort_key(x):
        return 10**18 if x[0] == float("inf") else x[0]

    delta_change_hits.sort(key=sort_key, reverse=True)

    if delta_change_hits:
        print(f"\n========== [ALERT] 증감량 변화 {int(DELTA_CHANGE_THRESHOLD*100)}% 이상 ==========")
        for ratio, name, prev_d, today_d in delta_change_hits:
            ratio_text = "∞" if ratio == float("inf") else f"{ratio*100:.2f}%"
            sp = "+" if prev_d >= 0 else ""
            st = "+" if today_d >= 0 else ""
            print(f"- {name}  (전날 Δ {sp}{fmt(prev_d)} → 오늘 Δ {st}{fmt(today_d)} / 변화 {ratio_text})")
    else:
        print(f"\n========== [ALERT] 증감량 변화 {int(DELTA_CHANGE_THRESHOLD*100)}% 이상 없음 ==========")

    # Slack 메시지
    lines = []
    lines.append(f"*📈 카카오 채널 친구수 리포트* ({today_str})")
    lines.append("")

    lines.append(f"*✅ TOP {TOP_N} 증가량*")
    for i, (delta, name, _friend_col, prev_friend, curr) in enumerate(top_deltas, start=1):
        sign = "+" if delta >= 0 else ""
        lines.append(f"*{i}. {name}* / {fmt(prev_friend)} → {fmt(curr)} / Δ {sign}{fmt(delta)}")

    lines.append("")
    lines.append(f"*✅ TOP {TOP_N} 증가율*")
    for i, (rate, name, _friend_col, prev_friend, curr, delta) in enumerate(top_rates, start=1):
        sign = "+" if delta >= 0 else ""
        lines.append(f"*{i}. {name}* / {fmt(prev_friend)} → {fmt(curr)} / Δ {sign}{fmt(delta)} / {rate*100:.2f}%")

    lines.append("")
    lines.append(f"*🚨 증감량 변화 {int(DELTA_CHANGE_THRESHOLD*100)}% 이상*")
    if delta_change_hits:
        for ratio, name, prev_d, today_d in delta_change_hits:
            ratio_text = "∞" if ratio == float("inf") else f"{ratio*100:.2f}%"
            sp = "+" if prev_d >= 0 else ""
            st = "+" if today_d >= 0 else ""
            lines.append(f"- *{name}* / 전날 Δ {sp}{fmt(prev_d)} → 오늘 Δ {st}{fmt(today_d)} (변화 {ratio_text})")
    else:
        lines.append("- 해당 없음")

    send_to_slack("\n".join(lines))
    print("\n[INFO] 출력 및 Slack 전송 완료")


if __name__ == "__main__":
    main()
