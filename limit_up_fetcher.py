"""
limit_up_fetcher.py
-------------------
간단한 KOSPI / KOSDAQ 상한가 종목 자동 추출 스크립트

● 기능
  1. pykrx 라이브러리를 사용하여 지정 날짜(기본=오늘)의 전 종목 시가/종가 데이터를 수집
  2. 당일 등락률이 +29.5% 이상인 종목을 '상한가'로 간주하여 필터링
  3. KOSPI, KOSDAQ 각각 1위(거래대금 기준) 종목을 선정
  4. 선택적으로 네이버 금융 뉴스 3개 헤드라인을 크롤링해 JSON으로 출력

● 사용 방법
  $ pip install pykrx beautifulsoup4 requests pandas

  # 당일 장 마감 후 실행
  $ python limit_up_fetcher.py
    → 결과가 up_stocks_YYYYMMDD.json 으로 저장됩니다.

  # 특정 날짜 조회
  $ python limit_up_fetcher.py 20250618

※ 한국거래소 API 지연(16:30~17:00) 시점에는 데이터가 비어 있을 수 있습니다.
"""

import sys
import json
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pykrx import stock


def get_limit_up_stocks(date: str) -> pd.DataFrame:
    """모든 상장 종목에서 등락률 +29.5% 이상 필터링"""
    tickers = stock.get_market_ticker_list(date)
    rows = []
    for t in tickers:
        df = stock.get_market_ohlcv_by_date(date, date, t)
        if df.empty:
            continue
        row = df.iloc[0]
        pct = row['등락률']
        if pct >= 29.5:
            rows.append({
                "ticker": t,
                "name": stock.get_market_ticker_name(t),
                "close": int(row['종가']),
                "open": int(row['시가']),
                "pct_chg": round(pct, 2),
                "volume": int(row['거래량']),
                "value": int(row['거래대금'])
            })
    return pd.DataFrame(rows)


def fetch_news_headlines(ticker: str, cnt: int = 3) -> List[str]:
    """네이버 금융 뉴스 헤드라인 상위 cnt 개 가져오기"""
    code = ticker
    url = f"https://finance.naver.com/item/news.naver?code={code}"
    res = requests.get(url, timeout=10)
    soup = BeautifulSoup(res.text, "html.parser")
    titles = [a.get_text(strip=True) for a in soup.select('.newsList li > a')]
    return titles[:cnt]


def main(date: str):
    df = get_limit_up_stocks(date)
    if df.empty:
        print("상한가 종목이 없습니다.")
        return

    # 거래대금 상위 1종목씩 선별
    kospi_df = df[df['ticker'].str.startswith(tuple('0 1 2 3 4 5'.split()))]  # 발행시장 구분 Rough
    kosdaq_df = df[~df['ticker'].str.startswith(tuple('0 1 2 3 4 5'.split()))]

    result: Dict[str, Dict] = {}
    for market_name, mdf in [("KOSPI", kospi_df), ("KOSDAQ", kosdaq_df)]:
        if not mdf.empty:
            top = mdf.sort_values('value', ascending=False).iloc[0].to_dict()
            top['news'] = fetch_news_headlines(top['ticker'])
            result[market_name] = top

    outfile = f"up_stocks_{date}.json"
    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Saved → {outfile}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_arg = sys.argv[1]
        try:
            datetime.strptime(date_arg, "%Y%m%d")
            target_date = date_arg
        except ValueError:
            print("날짜 형식은 YYYYMMDD")
            sys.exit(1)
    else:
        target_date = datetime.now().strftime("%Y%m%d")

    main(target_date)