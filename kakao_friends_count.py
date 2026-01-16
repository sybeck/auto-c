import re
import time
from datetime import datetime
from typing import Optional, List, Tuple

import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# =======================
# ✅ 설정 (여기만 수정)
# =======================
SERVICE_ACCOUNT_JSON = "service_account.json"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1J0muYgf29eqIMDe1BmYKTtS5-tP1KcV2M5ojv1WRHNw/edit?gid=0#gid=0"   # <-- 네 구글시트 URL
WORKSHEET_NAME = "시트1"                      # <-- 탭 이름

HEADER_ROW = 2          # 아이디가 들어있는 행
DATA_START_ROW = 3      # 날짜/데이터 시작 행 (A3부터)
DATE_COL = 1            # A열

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

SLEEP_BETWEEN = 1.0     # 과도 요청 방지
DATE_FORMAT = "%Y-%m-%d"  # '2026-01-16' 형태


def normalize_korean_number(text: str) -> Optional[int]:
    text = (text or "").strip().replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*만", text)
    if m:
        return int(float(m.group(1)) * 10000)
    m2 = re.search(r"(\d+)", text)
    return int(m2.group(1)) if m2 else None


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


def connect_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)

    if "PUT_YOUR_SHEET_URL_HERE" in GOOGLE_SHEET_URL:
        raise ValueError("GOOGLE_SHEET_URL을 실제 구글시트 URL로 바꿔주세요.")

    sh = gc.open_by_url(GOOGLE_SHEET_URL)
    ws = sh.worksheet(WORKSHEET_NAME)
    return ws


def get_header_ids(ws):
    """
    HEADER_ROW(예: 2행)에서
    '짝수번째 열(B, D, F, ...)' 중
    값이 있는 칸만 (col_index, kakao_id)로 반환
    """
    row_vals = ws.row_values(HEADER_ROW)
    results = []

    for col_idx, val in enumerate(row_vals, start=1):
        # 짝수 열만 (B=2, D=4, F=6 ...)
        if col_idx % 2 != 0:
            continue

        v = (val or "").strip()
        if not v:
            continue

        results.append((col_idx, v))

    return results



def find_next_empty_row_in_col_a(ws) -> int:
    """
    A3부터 아래로 첫 빈 행을 찾음.
    (중간에 빈칸이 있으면 거기를 사용)
    """
    col_vals = ws.col_values(DATE_COL)  # A열 전체(실제 값 있는 만큼)
    # col_vals 길이 < DATA_START_ROW-1 이면 바로 DATA_START_ROW가 비어있음
    for r in range(DATA_START_ROW, max(len(col_vals) + 2, DATA_START_ROW + 1)):
        v = ws.cell(r, DATE_COL).value
        if v is None or str(v).strip() == "":
            return r

    # fallback (일반적으로 여기 안 옴)
    return len(col_vals) + 1


def get_friend_count_playwright(page, kakao_id: str) -> Optional[int]:
    url = f"https://pf.kakao.com/{kakao_id}"
    page.goto(url, wait_until="networkidle", timeout=30000)
    html = page.content()
    return extract_friend_count_from_html(html)


def main():
    ws = connect_sheet()

    header_ids = get_header_ids(ws)
    if not header_ids:
        raise RuntimeError(f"{HEADER_ROW}행에 트래킹할 아이디가 없습니다. (예: B2, D2, E2...)")

    target_row = find_next_empty_row_in_col_a(ws)
    today_str = datetime.now().strftime(DATE_FORMAT)

    print(f"기록 행: {target_row}, 날짜: {today_str}")
    print("대상 아이디(열 순서):", header_ids)

    # 업데이트 셀 모으기
    updates: List[gspread.Cell] = []
    # 날짜 기록
    updates.append(gspread.Cell(target_row, DATE_COL, today_str))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=USER_AGENT)

        for col_idx, kakao_id in header_ids:
            try:
                cnt = get_friend_count_playwright(page, kakao_id)
                print(f"- col {col_idx}: {kakao_id} -> {cnt}")

                if cnt is None:
                    # 파싱 실패면 빈칸으로 두고 넘어감(원하면 'ERROR' 넣어도 됨)
                    continue

                # 아이디가 있는 같은 열에 친구수 기록
                updates.append(gspread.Cell(target_row, col_idx, cnt))

                time.sleep(SLEEP_BETWEEN)

            except Exception as e:
                print(f"  오류: {kakao_id} ({col_idx}열): {e}")

        browser.close()

    # 배치 업데이트
    ws.update_cells(updates, value_input_option="USER_ENTERED")
    print(f"완료: 날짜 1개 + 친구수 {len(updates)-1}개 기록")


if __name__ == "__main__":
    main()
