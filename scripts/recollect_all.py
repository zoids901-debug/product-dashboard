# -*- coding: utf-8 -*-
"""OK포스 5개 매장 일자별 데이터 깨끗하게 재수집 — 특정 기간.

배경: 옛 데이터는 is_valid() 패치 전 필터로 수집돼 '*' 베이커리 상품이 제외됨.
해결: 해당 기간 OK포스 5개 매장 일자별 데이터를 정상 재수집(패치된 필터)하여 완전 교체.
운정(TOSS)은 건드리지 않음.

실행: GitHub Actions (OK포스 IP 화이트리스트 우회).
"""
import os, sys, json, asyncio
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actions_sync import (okpos_login, okpos_fetch_day, gh_get, gh_put,
                          is_valid, normalize_name, STORES)

START = os.environ.get('RC_START') or '2025-01-01'
END   = os.environ.get('RC_END')   or datetime.now().strftime('%Y-%m-%d')
OKPOS_LOCS = {'가산', '다산', '수원', '하남', '광주'}


def daterange(s, e):
    sd = datetime.strptime(s, '%Y-%m-%d').date()
    ed = datetime.strptime(e, '%Y-%m-%d').date()
    cur = sd
    while cur <= ed:
        yield cur
        cur += timedelta(days=1)


async def main():
    session, csrf, savename = await okpos_login()
    stats = {'days': 0, 'updated': 0, 'total_net': 0, 'no_daily': 0}

    for d in daterange(START, END):
        date_str = d.strftime('%Y-%m-%d')
        stats['days'] += 1

        # OK포스 5개 매장 fetch → loc별 합산 (다산 1층+지하 같은 동일 loc 다중코드 합산)
        loc_bucket = {}    # loc -> {item: {...}}
        for si in STORES.values():
            loc = si['location']
            if loc not in OKPOS_LOCS:
                continue
            try:
                rows = okpos_fetch_day(session, csrf, savename, date_str, si['code'], si['name'])
            except Exception as e:
                print(f'  [ERR] {date_str} {si["code"]}/{loc}: {e}', flush=True)
                continue
            bucket = loc_bucket.setdefault(loc, {})
            for row in rows:
                nm = (row.get('PROD_NM') or '').strip()
                qty = int(row.get('SALE_QTY') or 0)
                net = int(row.get('TOT_SALE_AMT') or 0)
                if not is_valid(nm) or net == 0:
                    continue
                nm = normalize_name(nm)
                code = (row.get('PROD_CD') or '').strip()
                cb = (row.get('LCLS_NM') or '').strip()
                cm = (row.get('MCLS_NM') or '').strip()
                cs = (row.get('SCLS_NM') or '').strip()
                if nm in bucket:
                    bucket[nm]['qty'] += qty
                    bucket[nm]['net'] += net
                    if not bucket[nm].get('code') and code:
                        bucket[nm]['code'] = code
                    if not bucket[nm].get('cat_big') and cb:
                        bucket[nm]['cat_big'], bucket[nm]['cat_mid'], bucket[nm]['cat_small'] = cb, cm, cs
                else:
                    bucket[nm] = {'item': nm, 'code': code, 'qty': qty, 'net': net,
                                  'cat_big': cb, 'cat_mid': cm, 'cat_small': cs}

        # daily 파일의 OK포스 5개 loc 항목 완전 교체 (운정은 보존)
        fname = d.strftime('%y%m%d') + '.json'
        path = f'data/daily/{fname}'
        sha, existing = gh_get(path)
        if not existing:
            stats['no_daily'] += 1
            continue
        stores = existing.setdefault('stores', {})
        old_net = sum(int(x.get('net', 0)) for loc in OKPOS_LOCS for x in stores.get(loc, []))
        new_net = 0
        for loc in OKPOS_LOCS:
            items = list(loc_bucket.get(loc, {}).values())
            stores[loc] = items
            new_net += sum(int(x.get('net', 0)) for x in items)
        if old_net == new_net:
            continue
        gh_put(path, json.dumps(existing, ensure_ascii=False, separators=(',', ':')).encode('utf-8'),
               f'recollect: 5매장 {date_str} (old {old_net:,} -> new {new_net:,})', sha=sha)
        stats['updated'] += 1
        stats['total_net'] += new_net
        print(f'  [OK] {date_str}: 5매장 {new_net:,}원 (이전 {old_net:,})', flush=True)

    print()
    print('=' * 60)
    print(f'기간: {START} ~ {END}')
    print(f'처리 {stats["days"]}일 / 갱신 {stats["updated"]}일 / daily없음 {stats["no_daily"]}일')
    print(f'재수집 5매장 매출 합: {stats["total_net"]:,}원')


asyncio.run(main())
