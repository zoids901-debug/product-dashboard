# -*- coding: utf-8 -*-
"""V70585(광주 To go zone) 단독 일자별 fetch → 광주 daily에 합산 backfill.

배경: product-dashboard 옛 daily backfill이 V70577만 가져가서 V70585(KIA 홈경기일 매출) 전체 누락.
2024년 -3.23억 / 2025년 일부 / 2026년 일부 영향.

실행: GitHub Actions (OK포스 IP 화이트리스트 우회).
"""
import os, sys, json, asyncio, base64
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actions_sync import okpos_login, okpos_fetch_day, gh_get, gh_put, gh_exists, is_valid, normalize_name


V70585 = {'code': 'V70585', 'name': '인크커피광주(To go zone)'}
START = os.environ.get('BF_START', '2024-01-01')
END   = os.environ.get('BF_END',   datetime.now().strftime('%Y-%m-%d'))


def daterange(s, e):
    sd = datetime.strptime(s, '%Y-%m-%d').date()
    ed = datetime.strptime(e, '%Y-%m-%d').date()
    cur = sd
    while cur <= ed:
        yield cur
        cur += timedelta(days=1)


async def main():
    session, csrf, savename = await okpos_login()

    stats = {'total_days': 0, 'with_sales_days': 0, 'merged_days': 0,
             'total_net': 0, 'total_qty': 0, 'skipped_no_daily': []}

    for d in daterange(START, END):
        date_str = d.strftime('%Y-%m-%d')
        stats['total_days'] += 1
        try:
            rows = okpos_fetch_day(session, csrf, savename, date_str, V70585['code'], V70585['name'])
        except Exception as e:
            print(f'  [ERR] {date_str}: {e}', flush=True)
            continue
        if not rows:
            continue
        # 매출 0 아닌 row만
        valid_rows = []
        d_net = 0; d_qty = 0
        for row in rows:
            nm = (row.get('PROD_NM') or '').strip()
            qty = int(row.get('SALE_QTY') or 0)
            net = int(row.get('TOT_SALE_AMT') or 0)
            if not is_valid(nm) or net == 0: continue
            nm = normalize_name(nm)
            valid_rows.append({
                'item': nm, 'qty': qty, 'net': net,
                'cat_big':   (row.get('LCLS_NM') or '').strip(),
                'cat_mid':   (row.get('MCLS_NM') or '').strip(),
                'cat_small': (row.get('SCLS_NM') or '').strip(),
            })
            d_net += net; d_qty += qty
        if not valid_rows:
            continue
        stats['with_sales_days'] += 1

        # 광주 daily 파일에 합산
        fname = d.strftime('%y%m%d') + '.json'
        path = f'data/daily/{fname}'
        sha, existing = gh_get(path)
        if not existing:
            print(f'  [SKIP] {date_str}: daily 파일 없음 ({fname})', flush=True)
            stats['skipped_no_daily'].append(date_str)
            continue
        stores = existing.get('stores', {})
        gj = stores.get('광주', [])
        # 이미 같은 PROD_NM(V70585 행) 있는지 — bucket_dict 형태로 merge
        bucket = {x['item']: x for x in gj}
        for r in valid_rows:
            nm = r['item']
            if nm in bucket:
                bucket[nm]['qty'] += r['qty']
                bucket[nm]['net'] += r['net']
                if not bucket[nm].get('cat_big') and r['cat_big']:
                    bucket[nm]['cat_big']   = r['cat_big']
                    bucket[nm]['cat_mid']   = r['cat_mid']
                    bucket[nm]['cat_small'] = r['cat_small']
            else:
                bucket[nm] = r
        stores['광주'] = list(bucket.values())
        existing['stores'] = stores

        gh_put(path, json.dumps(existing, ensure_ascii=False, separators=(',',':')).encode('utf-8'),
               f'backfill: V70585 {date_str} (+{len(valid_rows)}items / +{d_net:,}원)', sha=sha)
        stats['merged_days'] += 1
        stats['total_net'] += d_net
        stats['total_qty'] += d_qty
        print(f'  [OK] {date_str}: +{len(valid_rows)}items / +{d_qty}qty / +{d_net:,}원', flush=True)

    print()
    print('=' * 60)
    print(f'기간: {START} ~ {END}')
    print(f'총 일수: {stats["total_days"]}일')
    print(f'V70585 매출 있는 일수: {stats["with_sales_days"]}일')
    print(f'daily 파일에 합쳐진 일수: {stats["merged_days"]}일')
    print(f'합산된 매출: {stats["total_net"]:,}원 / {stats["total_qty"]}개')
    if stats['skipped_no_daily']:
        print(f'⚠️  daily 파일 없음 (skip): {len(stats["skipped_no_daily"])}일')
        print(f'   샘플: {stats["skipped_no_daily"][:5]}')

asyncio.run(main())
