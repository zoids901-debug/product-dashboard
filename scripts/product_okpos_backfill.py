# -*- coding: utf-8 -*-
"""상품 데이터 검증용 OK포스 백필 — 매장×상품 1년치 합계 raw.
- actions_sync.py의 okpos_login 재사용
- okpos_fetch_day의 date1_1=시작일, date1_2=끝일 인자로 일자 범위 fetch 시도
- 1년 범위로 받으면 결과는 매장×상품 누적 합 (또는 일별 합)

출력: data/raw_okpos_yearly/{YYYY}.json
"""
import sys, os, json, asyncio
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent))
from actions_sync import okpos_login, STORES, API_URL


def okpos_fetch_range(session, csrf, savename, date_from, date_to, code, name):
    """okpos_fetch_day의 일자 범위 버전.
    date1_1, date1_2를 다르게 주고 date_period1='1' (또는 기간) 시도."""
    shop_info = json.dumps([{"SHOP_CD": code, "SHOP_NM": name}])
    data = {
        csrf["key"]: csrf["val"],
        "S_CONTROLLER": "sale.sale.prod011", "S_METHOD": "search",
        "SHEETSEQ": "1", "S_SAVENAME": savename, "ss_PROD_FG": "N",
        "date1_1": date_from, "date1_2": date_to, "date_period1": "1",
        "ss_CLS_TEXT": "전체", "ss_SHOP_CD": code, "ss_SHOP_NM": name,
        "ss_SHOP_INFO": shop_info, "ss_VENDOR_NM": "전체",
        "ss_VENDOR_INFO": "[]", "ss_PAGE_NO1": "1",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Referer": "https://okasp.okpos.co.kr/sale/sale/prod011.jsp",
               "Origin":  "https://okasp.okpos.co.kr"}
    r = session.post(API_URL, data=data, headers=headers, timeout=120).json()
    if r.get("Result", {}).get("Code", 0) < 0:
        raise ValueError(r["Result"].get("Message", "API 오류"))
    return r.get("Data", [])


async def fetch_year(year):
    print(f'\n=== {year}년 상품 OK포스 fetch ===')
    today = date.today()
    end_date = date(year, 12, 31)
    if end_date > today:
        end_date = today
    start_iso = f'{year}-01-01'
    end_iso = end_date.isoformat()
    print(f'기간: {start_iso} ~ {end_iso}')

    session, csrf, savename = await okpos_login()
    print(f'  로그인 OK')

    result = {
        "year": str(year),
        "period": [start_iso, end_iso],
        "stores": {},
        "totals": {},
    }

    locations_done = set()  # 매장명 기준 (다산 1층/지하 같은 다중 코드 처리)
    for si in STORES.values():
        loc = si['location']
        code = si['code']
        name = si['name']
        try:
            print(f'  fetch [{loc}/{name}] {code}...', flush=True)
            rows = okpos_fetch_range(session, csrf, savename, start_iso, end_iso, code, name)
            print(f'    → {len(rows)}건')
            if loc not in result["stores"]:
                result["stores"][loc] = {}
            # 매장 안에서 같은 상품명이면 합산 (다중 코드/일자 누적)
            bucket = result["stores"][loc]
            for row in rows:
                item = (row.get('PROD_NM') or '').strip()
                if not item: continue
                qty = int(row.get('SALE_QTY') or 0)
                net = int(row.get('TOT_SALE_AMT') or 0)
                if item not in bucket:
                    bucket[item] = {"qty": 0, "net": 0}
                bucket[item]["qty"] += qty
                bucket[item]["net"] += net
        except Exception as e:
            print(f'    ✗ 실패: {e}')

    # 매장별 totals
    for loc, items in result["stores"].items():
        qty_sum = sum(v["qty"] for v in items.values())
        net_sum = sum(v["net"] for v in items.values())
        result["totals"][loc] = {
            "item_count": len(items),
            "qty_sum": qty_sum,
            "net_sum": net_sum,
        }

    # 저장
    out_dir = Path('data/raw_okpos_yearly')
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'{year}.json'
    # stores를 list로 변환 (dict 정렬 + JSON 가독성)
    out_data = {
        "year": result["year"],
        "period": result["period"],
        "totals": result["totals"],
        "stores": {loc: [{"item": k, **v} for k, v in sorted(items.items(), key=lambda x: -x[1]["net"])]
                   for loc, items in result["stores"].items()},
    }
    out_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'\n=== 매장별 합계 ===')
    for loc, t in result["totals"].items():
        print(f'  {loc}: 상품 {t["item_count"]}개 / qty {t["qty_sum"]:,} / net {t["net_sum"]:,}')
    print(f'\n→ 저장: {out_path}')
    return True


async def main():
    if len(sys.argv) < 2:
        print('Usage: python product_okpos_backfill.py YYYY [YYYY ...]')
        sys.exit(1)
    years = [int(y) for y in sys.argv[1:]]
    for year in years:
        try:
            await fetch_year(year)
        except Exception as e:
            print(f'[{year}] 실패: {e}')


if __name__ == '__main__':
    asyncio.run(main())
