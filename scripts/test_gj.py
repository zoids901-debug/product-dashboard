# -*- coding: utf-8 -*-
import sys, os, json, asyncio
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from actions_sync import okpos_login, API_URL


async def fetch_one(label, shop_info_list, sale_year):
    session, csrf, savename = await okpos_login()
    shop_info = json.dumps(shop_info_list)
    data = {
        csrf["key"]: csrf["val"],
        "S_CONTROLLER": "sale.sale.prod011", "S_METHOD": "search",
        "SHEETSEQ": "1", "S_SAVENAME": savename, "ss_PROD_FG": "N",
        "date1_1": str(sale_year) + "-01-01",
        "date1_2": str(sale_year) + "-12-31",
        "date_period1": "1",
        "ss_CLS_TEXT": "전체",
        "ss_SHOP_CD": ",".join(s["SHOP_CD"] for s in shop_info_list),
        "ss_SHOP_NM": label,
        "ss_SHOP_INFO": shop_info,
        "ss_VENDOR_NM": "전체", "ss_VENDOR_INFO": "[]", "ss_PAGE_NO1": "1",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Referer": "https://okasp.okpos.co.kr/sale/sale/prod011.jsp",
               "Origin":  "https://okasp.okpos.co.kr"}
    r = session.post(API_URL, data=data, headers=headers, timeout=180).json()
    rows = r.get("Data", [])
    total_net = sum(int(row.get("TOT_SALE_AMT") or 0) for row in rows)
    total_qty = sum(int(row.get("SALE_QTY") or 0) for row in rows)
    msg = "[" + label + "] 상품 " + str(len(rows)) + "개 / qty " + format(total_qty, ",") + " / net " + format(total_net, ",") + "원"
    print(msg)
    return total_net


async def main():
    a = await fetch_one("V70577_only",
                        [{"SHOP_CD": "V70577", "SHOP_NM": "광주_챔피언스"}], 2024)
    b = await fetch_one("V70585_only",
                        [{"SHOP_CD": "V70585", "SHOP_NM": "광주_ToGo"}], 2024)
    c = await fetch_one("V70577+V70585", [
        {"SHOP_CD": "V70577", "SHOP_NM": "광주_챔피언스"},
        {"SHOP_CD": "V70585", "SHOP_NM": "광주_ToGo"},
    ], 2024)
    print("")
    print("=== 비교 ===")
    print("V70577 단독 + V70585 단독 = " + format(a, ",") + " + " + format(b, ",") + " = " + format(a + b, ","))
    print("V70577+V70585 한 번에     = " + format(c, ","))
    print("yearly raw 광주 (저장된 기존) = 3,437,638,700")
    print("product 광주 2024           = 3,114,675,300")


asyncio.run(main())
