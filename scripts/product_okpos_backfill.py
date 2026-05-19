# -*- coding: utf-8 -*-
"""상품 검증용 OK포스 backfill — 매장×상품 합계 raw.
- 연 단위(YYYY) 또는 단일 월(YYYY-MM) 인자 지원
- actions_sync.py의 okpos_login + STORES + API_URL 재사용
- 결과: data/raw_okpos_yearly/{YYYY}.json 또는 data/raw_okpos_monthly/{YYYY-MM}.json
"""
import sys, os, json, asyncio, calendar
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent))
from actions_sync import okpos_login, STORES, API_URL


def okpos_fetch_range(session, csrf, savename, date_from, date_to, code, name):
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


async def _fetch_period(start_iso, end_iso, out_path, label):
    print()
    print("=== " + label + " ===")
    print("기간: " + start_iso + " ~ " + end_iso)

    session, csrf, savename = await okpos_login()
    print("  로그인 OK")

    stores_data = {}
    for si in STORES.values():
        loc = si["location"]
        code = si["code"]
        name = si["name"]
        try:
            print("  fetch [" + loc + "/" + name + "] " + code + "...", flush=True)
            rows = okpos_fetch_range(session, csrf, savename, start_iso, end_iso, code, name)
            print("    -> " + str(len(rows)) + "건")
            if loc not in stores_data:
                stores_data[loc] = {}
            bucket = stores_data[loc]
            for row in rows:
                item = (row.get("PROD_NM") or "").strip()
                if not item: continue
                qty = int(row.get("SALE_QTY") or 0)
                net = int(row.get("TOT_SALE_AMT") or 0)
                if item not in bucket:
                    bucket[item] = {"qty": 0, "net": 0}
                bucket[item]["qty"] += qty
                bucket[item]["net"] += net
        except Exception as e:
            print("    실패: " + str(e))

    totals = {}
    for loc, items in stores_data.items():
        totals[loc] = {
            "item_count": len(items),
            "qty_sum": sum(v["qty"] for v in items.values()),
            "net_sum": sum(v["net"] for v in items.values()),
        }

    out_dir = out_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    out_data = {
        "period": [start_iso, end_iso],
        "totals": totals,
        "stores": {loc: [{"item": k, **v} for k, v in sorted(items.items(), key=lambda x: -x[1]["net"])]
                   for loc, items in stores_data.items()},
    }
    out_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print("=== 매장별 합 ===")
    for loc, t in totals.items():
        print("  " + loc + ": 상품 " + str(t["item_count"]) + "개 / qty " + format(t["qty_sum"], ",") + " / net " + format(t["net_sum"], ","))
    print()
    print("-> 저장: " + str(out_path))
    return True


async def fetch_year(year):
    today = date.today()
    end_date = date(year, 12, 31)
    if end_date > today:
        end_date = today
    start_iso = str(year) + "-01-01"
    end_iso = end_date.isoformat()
    out_path = Path("data/raw_okpos_yearly") / (str(year) + ".json")
    return await _fetch_period(start_iso, end_iso, out_path, str(year) + "년 fetch")


async def fetch_month(yyyy_mm):
    y, m = map(int, yyyy_mm.split("-"))
    last_day = calendar.monthrange(y, m)[1]
    today = date.today()
    if y == today.year and m == today.month:
        last_day = today.day
    start_iso = "%04d-%02d-01" % (y, m)
    end_iso = "%04d-%02d-%02d" % (y, m, last_day)
    out_path = Path("data/raw_okpos_monthly") / (yyyy_mm + ".json")
    return await _fetch_period(start_iso, end_iso, out_path, yyyy_mm + " 단일 월 fetch (진단)")


def expand_args(args):
    """인자 확장 — 월 범위, 연도 범위, 단일."""
    # 두 인자 모두 YYYY-MM → 월 범위
    if len(args) == 2 and len(args[0]) == 7 and len(args[1]) == 7 and '-' in args[0] and '-' in args[1]:
        y1, m1 = map(int, args[0].split('-'))
        y2, m2 = map(int, args[1].split('-'))
        out = []
        y, m = y1, m1
        while (y, m) <= (y2, m2):
            out.append(("month", "%04d-%02d" % (y, m)))
            m += 1
            if m > 12: m = 1; y += 1
        return out
    # 두 인자 모두 YYYY → 연도 범위
    if len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        y1, y2 = int(args[0]), int(args[1])
        return [("year", y) for y in range(min(y1,y2), max(y1,y2)+1)]
    # 그 외: 각 인자 개별 처리
    out = []
    for arg in args:
        if "-" in arg and len(arg) == 7:
            out.append(("month", arg))
        elif arg.isdigit():
            out.append(("year", int(arg)))
    return out


async def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python product_okpos_backfill.py YYYY              # 연도 전체")
        print("  python product_okpos_backfill.py YYYY YYYY         # 연도 범위")
        print("  python product_okpos_backfill.py YYYY-MM           # 단일 월")
        print("  python product_okpos_backfill.py YYYY-MM YYYY-MM   # 월 범위")
        sys.exit(1)
    targets = expand_args(sys.argv[1:])
    print("=== 처리 대상: " + str(len(targets)) + "개 ===")
    for kind, val in targets:
        try:
            if kind == "month":
                await fetch_month(val)
            else:
                await fetch_year(val)
        except Exception as e:
            print("[" + str(val) + "] 실패: " + str(e))


if __name__ == "__main__":
    asyncio.run(main())
