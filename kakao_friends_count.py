import re
import time
from datetime import datetime
from typing import Optional, List, Tuple, Dict

import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# =======================
# ✅ 설정 (여기만 수정)
# =======================
SERVICE_ACCOUNT_JSON = "service_account.json"

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1J0muYgf29eqIMDe1BmYKTtS5-tP1KcV2M5ojv1WRHNw/edit?gid=0#gid=0"   # <-- 네 구글시트 URL
WORKSHEET_NAME = "시트1"

NAME_ROW = 1            # ✅ 1행: 이름(브랜드명/채널명)
HEADER_ROW = 2          # ✅ 2행: 카카오 채널 ID (짝수열만)
DATA_START_ROW = 3      # ✅ A3부터 날짜/데이터
DATE_COL = 1            # ✅ A열: 날짜

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

SLEEP_BETWEEN = 1.0     # ✅ URL 1개마다 1초 지연
DATE_FORMAT = "%Y-%m-%d"


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


def get_header_ids_even_cols(ws) -> List[Tuple[int, str]]:
    """
    2행에서 '짝수열(B,D,F,...)' 중 값이 있는 칸만 대상으로 반환
    (col_index, kakao_id)
    """
    row_vals = ws.row_values(HEADER_ROW)
    results: List[Tuple[int, str]] = []

    for col_idx, val in enumerate(row_vals, start=1):
        if col_idx % 2 != 0:   # ✅ 짝수열만
            continue
        v = (val or "").strip()
        if not v:
            continue
        results.append((col_idx, v))
    return results


def find_next_empty_row_in_col_a(ws) -> int:
    """
    A3부터 아래로 첫 빈 행 찾기
    """
    # col_values는 '값이 있는 만큼'만 오기 때문에, 중간 빈칸 탐색은 cell로 체크
    col_vals = ws.col_values(DATE_COL)

    r = DATA_START_ROW
    while True:
        v = ws.cell(r, DATE_COL).value
        if v is None or str(v).strip() == "":
            return r
        r += 1


def find_previous_filled_row(ws, current_row: int) -> Optional[int]:
    """
    current_row 바로 위부터 위로 올라가며 A열(날짜)이 채워진 가장 가까운 행 찾기
    """
    r = current_row - 1
    while r >= DATA_START_ROW:
        v = ws.cell(r, DATE_COL).value
        if v is not None and str(v).strip() != "":
            return r
        r -= 1
    return None


def get_friend_count_playwright(page, kakao_id: str) -> Optional[int]:
    url = f"https://pf.kakao.com/{kakao_id}"
    page.goto(url, wait_until="networkidle", timeout=30000)
    html = page.content()
    return extract_friend_count_from_html(html)


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


def main():
    ws = connect_sheet()

    targets = get_header_ids_even_cols(ws)
    if not targets:
        raise RuntimeError("2행(HEADER_ROW) 짝수열에 트래킹할 ID가 없습니다. (B2, D2, F2...)")

    # 이번에 쓸 행
    target_row = find_next_empty_row_in_col_a(ws)
    today_str = datetime.now().strftime(DATE_FORMAT)

    # 바로 위(이전 기록) 행
    prev_row = find_previous_filled_row(ws, target_row)

    print(f"[INFO] 기록 행: {target_row}, 날짜: {today_str}")
    print(f"[INFO] 이전 비교 행: {prev_row if prev_row else '없음(첫 기록)'}")
    print(f"[INFO] 대상 수: {len(targets)}")

    # 이름(1행)도 같이 가져오기
    # {col_idx: name}
    name_map: Dict[int, str] = {}
    for col_idx, _ in targets:
        nm = ws.cell(NAME_ROW, col_idx).value
        nm = (nm or "").strip()
        name_map[col_idx] = nm if nm else f"(col {col_idx})"

    # 이번 실행에서 읽은 친구수 저장
    current_counts: Dict[int, int] = {}  # {col_idx: count}

    updates: List[gspread.Cell] = []
    updates.append(gspread.Cell(target_row, DATE_COL, today_str))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=USER_AGENT)

        for col_idx, kakao_id in targets:
            try:
                cnt = get_friend_count_playwright(page, kakao_id)
                print(f"- {name_map[col_idx]} / {kakao_id} -> {cnt}")

                if cnt is not None:
                    current_counts[col_idx] = cnt
                    updates.append(gspread.Cell(target_row, col_idx, cnt))

                time.sleep(SLEEP_BETWEEN)

            except Exception as e:
                print(f"[ERROR] {name_map[col_idx]} / {kakao_id} (col {col_idx}): {e}")

        browser.close()

    # 시트 기록(날짜 + 친구수)
    ws.update_cells(updates, value_input_option="USER_ENTERED")
    print(f"[INFO] 저장 완료: 날짜 1개 + 친구수 {len(updates)-1}개")

    # =========================
    # ✅ 증가량/증가율 TOP 10 출력
    # =========================
    if prev_row is None:
        print("[RANK] 이전 행이 없어 증가량/증가율 계산을 건너뜁니다.")
        return

    deltas = []   # (delta, name, col_idx, prev, curr)
    rates = []    # (rate, name, col_idx, prev, curr, delta)

    for col_idx, kakao_id in targets:
        curr = current_counts.get(col_idx)
        if curr is None:
            continue

        prev_val = safe_int(ws.cell(prev_row, col_idx).value)
        if prev_val is None:
            continue

        delta = curr - prev_val

        # 증가량 랭킹(내림차순)
        deltas.append((delta, name_map[col_idx], col_idx, prev_val, curr))

        # 증가율: prev가 0이면 계산 불가(무한대) → 제외(원하면 별도 처리 가능)
        if prev_val > 0:
            rate = delta / prev_val
            rates.append((rate, name_map[col_idx], col_idx, prev_val, curr, delta))

    # TOP 10 뽑기
    deltas.sort(key=lambda x: x[0], reverse=True)
    rates.sort(key=lambda x: x[0], reverse=True)

    top_deltas = deltas[:10]
    top_rates = rates[:10]

    print("\n========== [TOP 10] 증가량(Δ) ==========")
    for i, (delta, name, col_idx, prev_val, curr) in enumerate(top_deltas, start=1):
        sign = "+" if delta >= 0 else ""
        print(f"{i:02d}. {name}  {prev_val} -> {curr}  (Δ {sign}{delta})")

    print("\n========== [TOP 10] 증가율(Δ/이전) ==========")
    for i, (rate, name, col_idx, prev_val, curr, delta) in enumerate(top_rates, start=1):
        sign = "+" if delta >= 0 else ""
        print(f"{i:02d}. {name}  {prev_val} -> {curr}  (Δ {sign}{delta}, {rate*100:.2f}%)")

    print("\n[RANK] 출력 완료 (cron.log에 누적됩니다)")


if __name__ == "__main__":
    main()
