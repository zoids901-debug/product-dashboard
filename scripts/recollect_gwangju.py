# -*- coding: utf-8 -*-
"""광주 일자별 데이터 깨끗하게 재수집 — V70577(챔피언스) + V70585(투고존) 각각 fetch.

배경: V70585 백필이 2025/2026 일자별엔 이미 투고존이 있던 데이터에 중복 합산됨.
해결: 해당 기간 광주 일자별 데이터를 OK포스에서 정상 재수집하여 완전 교체.

실행: GitHub Actions (OK포스 IP 화이트리스트 우회).
"""
import os, sys, json, asyncio
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from actions_sync import okpos_login, okpos_fetch_day, gh_get, gh_put, gh_exists, is_valid, normalize_name

GWANGJU = [
    {'code': 'V70577', 'name': '인크커피광주기아챔피언스필드점'},
    {'code': 'V70585', 'name': '인크커피광주(To go zone)'},
]
START = os.environ.get('RC_START') or '2025-01-01'
END   = os.environ.get('RC_END')   or datetime.now().strftime('%Y-%m-%d')


def daterange(s, e):
    sd = datetime.strptime(s, '%Y-%m-%d').date()
    ed = datetime.strptime(e, '%Y-%m-%d').date()
    cur = sd
    while cur <= ed:
        yield cur
        cur += timedelta(days=1)


async def main():
    session, csrf, savename = await okpos_login()
    stats = {'days': 0, 'updated': 0, 'total_net': 0, 'no_daily': []}

    for d in daterange(START, END):
        date_str = d.strftime('%Y-%m-%d')
        stats['days'] += 1

        # 광주 두 매장 코드 각각 fetch → 상품별 합산
        bucket = {}
        day_net = 0
        for shop in GWANGJU:
            try:
                rows = okpos_fetch_day(session, csrf, savename, date_str, shop['code'], shop['name'])
            except Exception as e:
                print(f'  [ERR] {date_str} {shop["code"]}: {e}', flush=True)
                continue
            for row in rows:
                nm = (row.get('PROD_NM') or '').strip()
                qty = int(row.get('SALE_QTY') or 0)
                net = int(row.get('TOT_SALE_AMT') or 0)
                if not is_valid(nm) or net == 0:
                    continue
                nm = normalize_name(nm)
                cb = (row.get('LCLS_NM') or '').strip()
                cm = (row.get('MCLS_NM') or '').strip()
                cs = (row.get('SCLS_NM') or '').strip()
                if nm in bucket:
                    bucket[nm]['qty'] += qty
                    bucket[nm]['net'] += net
                    if not bucket[nm].get('cat_big') and cb:
                        bucket[nm]['cat_big'], bucket[nm]['cat_mid'], bucket[nm]['cat_small'] = cb, cm, cs
                else:
                    bucket[nm] = {'item': nm, 'qty': qty, 'net': net,
                                  'cat_big': cb, 'cat_mid': cm, 'cat_small': cs}
                day_net += net

        # daily 파일의 광주 항목을 완전 교체
        fname = d.strftime('%y%m%d') + '.json'
        path = f'data/daily/{fname}'
        sha, existing = gh_get(path)
        if not existing:
            stats['no_daily'].append(date_str)
            continue
        new_gj = list(bucket.values())
        old_gj = existing.get('stores', {}).get('광주', [])
        old_net = sum(int(x.get('net', 0)) for x in old_gj)
        if old_net == day_net and len(old_gj) == len(new_gj):
            continue  # 변화 없음 → skip
        existing.setdefault('stores', {})['광주'] = new_gj
        gh_put(path, json.dumps(existing, ensure_ascii=False, separators=(',', ':')).encode('utf-8'),
               f'recollect: 광주 {date_str} (old {old_net:,} -> new {day_net:,})', sha=sha)
        stats['updated'] += 1
        stats['total_net'] += day_net
        print(f'  [OK] {date_str}: 광주 {len(new_gj)}품목 / {day_net:,}원 (이전 {old_net:,})', flush=True)

    print()
    print('=' * 60)
    print(f'기간: {START} ~ {END}')
    print(f'처리 일수: {stats["days"]}일 / 갱신 {stats["updated"]}일')
    print(f'재수집 광주 매출 합: {stats["total_net"]:,}원')
    if stats['no_daily']:
        print(f'daily 파일 없음: {len(stats["no_daily"])}일 ({stats["no_daily"][:5]})')


asyncio.run(main())
