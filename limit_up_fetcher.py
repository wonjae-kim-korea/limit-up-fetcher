"""
limit_up_fetcher.py
-------------------
KOSPI / KOSDAQ 상한가(+29.5%↑) 종목을 자동 추출해
• 거래대금(= 종가 × 거래량) 기준 상위 1종목씩
• 네이버 금융 뉴스 헤드라인 3개
를 JSON 파일로 저장합니다.

사용:
    python limit_up_fetcher.py           # 오늘 날짜
    python limit_up_fetcher.py YYYYMMDD  # 특정 날짜
필수 패키지: pykrx pandas requests beautifulsoup4
"""

import sys
import json
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock


# ────────────────────────────────────────────────
# 1. 오늘 상한가(+29.5%↑) 종목 조회
# ────────────────────────────────────────────────
def get_limit_up_stocks(date: str) -> pd.DataFrame:
    rows = []
    for t in stock.get_market_ticker_list(date):
        df = stock.get_market_ohlcv_by_date(date, date, t)
        if df.empty:
            continue

        pct = df.iloc[0]["등락률"]          # 등락률 %
        if pct >= 29.5:                     # 상한가 판정
            row = df.iloc[0]
            rows.append({
                "ticker":  t,
                "name":    stock.get_market_ticker_name(t),
                "close":   int(row["종가"]),
                "open":    int(row["시가"]),
                "pct_chg": round(float(pct), 2),
                "volume":  int(row["거래량"]),
                "value":   int(row["종가"] * row["거래량"])   # 거래대금(원)
            })
    return pd.DataFrame(rows)


# ────────────────────────────────────────────────
# 2. 네이버 금융 뉴스 헤드라인 상위 3개
# ────────────────────────────────────────────────
def fetch_news_headlines(ticker: str, cnt: int = 3) -> List[str]:
    url = f"https://finance.naver.com/item/news.naver?code={ticker}"
    soup = BeautifulSoup(requests.get(url, timeout=10).text, "html.parser")
    return [a.get_text(strip=True) for a in soup.select(".newsList li > a")][:cnt]


# ────────────────────────────────────────────────
# 3. 메인 로직: DataFrame → JSON 저장
# ────────────────────────────────────────────────
def main(date: str):
    df = get_limit_up_stocks(date)
    if df.empty:
        print("상한가 종목이 없습니다.")
        return

    kospi = df[df["ticker"].str.startswith(tuple("012345"))]
    kosdaq = df[~df["ticker"].str.startswith(tuple("012345"))]

    result: Dict[str, Dict] = {}
    for name, mdf in [("KOSPI", kospi), ("KOSDAQ", kosdaq)]:
        if not mdf.empty:
            top = mdf.sort_values("value", ascending=False).iloc[0].to_dict()
            top["news"] = fetch_news_headlines(top["ticker"])
            result[name] = top

    out = f"up_stocks_{date}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Saved → {out}")


# ────────────────────────────────────────────────
# 4. CLI 엔트리포인트
# ────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            datetime.strptime(sys.argv[1], "%Y%m%d")
            target_date = sys.argv[1]
        except ValueError:
            sys.exit("날짜 형식은 YYYYMMDD")
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    main(target_date)
