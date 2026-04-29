# -*- coding: utf-8 -*-
"""GH Actions에서 실행되는 통합 일일 동기화
- TOSS 운정점: ID/PW로 로그인 → 헤더 캡쳐 → 일별 데이터 수집
- OK POS 6개 매장: ID/PW로 로그인 → 일별 데이터 수집
- GitHub repo (data/daily/, data/2604.json) 자동 commit
환경변수:
  GH_TOKEN, GH_REPO (예: zoids901-debug/product-dashboard)
  OKPOS_ID, OKPOS_PW
  TOSS_ID (휴대폰번호), TOSS_PW
"""
import os, sys, io, json, base64, asyncio, calendar, time
import urllib.request, urllib.error
from datetime import date, timedelta, datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
from playwright.async_api import async_playwright

GH_TOKEN = os.environ['GH_TOKEN']
GH_REPO  = os.environ.get('GH_REPO', 'zoids901-debug/product-dashboard')
GH_HEADERS = {'Authorization': f'token {GH_TOKEN}', 'User-Agent': 'gh-actions/1.0'}

OKPOS_ID = os.environ['OKPOS_ID']
OKPOS_PW = os.environ['OKPOS_PW']
TOSS_ID  = os.environ['TOSS_ID']
TOSS_PW  = os.environ['TOSS_PW']

LOOKBACK_DAYS = 7

# OK POS 매장
STORES = {
    "가산":          {"code": "V09651", "name": "인크커피(가산점)",             "location": "가산"},
    "다산_1층":      {"code": "V67293", "name": "인크커피다산1호점(1층)",        "location": "다산"},
    "다산_지하1":    {"code": "V67295", "name": "인크커피다산1호점(지하)",        "location": "다산"},
    "수원":          {"code": "V68581", "name": "인크커피스타필드수원점",         "location": "수원"},
    "하남":          {"code": "V00555", "name": "인크커피(하남미사1호점)",        "location": "하남"},
    "광주_챔피언스": {"code": "V70577", "name": "인크커피광주기아챔피언스필드점", "location": "광주"},
    "광주_ToGo":     {"code": "V70585", "name": "인크커피광주(To go zone)",      "location": "광주"},
}

LOGIN_URL = "https://asp.netusys.com/login/login_form.jsp"
API_URL   = "https://okasp.okpos.co.kr/sale/sale/ddd.htmlSheetAction"
PROD_PAGE = "https://okasp.okpos.co.kr/sale/sale/prod010.jsp"

TOSS_BASE = 'https://api-public.tossplace.com'
TOSS_MERCHANT_ID = 304265


# ── GitHub ───────────────────────────────────────
def gh_get(path):
    api = f'https://api.github.com/repos/{GH_REPO}/contents/{path}'
    try:
        with urllib.request.urlopen(urllib.request.Request(api, headers=GH_HEADERS), timeout=15) as r:
            d = json.loads(r.read())
            return d['sha'], json.loads(base64.b64decode(d['content']).decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 404: return None, None
        raise

def gh_exists(path):
    sha, _ = gh_get(path); return sha

def gh_put(path, content_bytes, message, sha=None):
    api = f'https://api.github.com/repos/{GH_REPO}/contents/{path}'
    for _ in range(3):
        if sha is None: sha = gh_exists(path)
        body = {'message': message, 'content': base64.b64encode(content_bytes).decode()}
        if sha: body['sha'] = sha
        req = urllib.request.Request(api, data=json.dumps(body).encode(),
            headers={**GH_HEADERS, 'Content-Type': 'application/json'}, method='PUT')
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status
        except urllib.error.HTTPError as e:
            if e.code == 409:
                time.sleep(1); sha = None; continue
            raise


# ── OK POS ────────────────────────────────────────
async def okpos_login():
    print('[OKPOS] 로그인 중...', flush=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-popup-blocking"])
        page = await browser.new_page()
        page.on("popup", lambda popup: asyncio.ensure_future(popup.close()))
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        await page.evaluate("window.checkPopAccept = true;")
        await page.fill("#user_id", OKPOS_ID)
        await page.fill("#user_pwd", OKPOS_PW)
        await page.evaluate("doSubmit();")
        await asyncio.sleep(6)
        mf = page.frame(name="MainFrm")
        await mf.goto(PROD_PAGE, wait_until="networkidle")
        await asyncio.sleep(3)
        inner = next((f for f in page.frames if "prod011" in f.url), None)
        csrf = await inner.evaluate("""
            (function(){let el=document.querySelector("input[data-dchk='N']");
            return el?{key:el.name,val:el.value}:null;})()
        """)
        savename = await inner.evaluate("""
            (function(){let el=document.getElementById('S_SAVENAME');return el?el.value:'';})()
        """)
        cookies = await page.context.cookies()
        await browser.close()
    session = requests.Session()
    for c in cookies:
        session.cookies.set(c["name"], c["value"], domain=c.get("domain", "okasp.okpos.co.kr"))
    print('[OKPOS] 완료', flush=True)
    return session, csrf, savename

def okpos_fetch_day(session, csrf, savename, date_str, code, name):
    shop_info = json.dumps([{"SHOP_CD": code, "SHOP_NM": name}])
    data = {
        csrf["key"]: csrf["val"],
        "S_CONTROLLER": "sale.sale.prod011", "S_METHOD": "search",
        "SHEETSEQ": "1", "S_SAVENAME": savename, "ss_PROD_FG": "N",
        "date1_1": date_str, "date1_2": date_str, "date_period1": "1",
        "ss_CLS_TEXT": "전체", "ss_SHOP_CD": code, "ss_SHOP_NM": name,
        "ss_SHOP_INFO": shop_info, "ss_VENDOR_NM": "전체",
        "ss_VENDOR_INFO": "[]", "ss_PAGE_NO1": "1",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Referer": "https://okasp.okpos.co.kr/sale/sale/prod011.jsp",
               "Origin":  "https://okasp.okpos.co.kr"}
    r = session.post(API_URL, data=data, headers=headers, timeout=60).json()
    if r.get("Result", {}).get("Code", 0) < 0:
        raise ValueError(r["Result"].get("Message", "API 오류"))
    return r.get("Data", [])


def is_valid(name):
    if not name or len(name) < 2: return False
    if any(c in name for c in ['-','*','=']): return False
    return True


# 매장간 상품명 표기 차이 통일 (잘못된 표기 → 정식 표기)
NAME_ALIASES = {
    '애플 잼 스콘': '애플잼 스콘',
}

def normalize_name(name):
    return NAME_ALIASES.get(name, name)


# ── TOSS ──────────────────────────────────────────
async def toss_login():
    """playwright UI 로그인 → API 호출 캡쳐로 헤더 추출"""
    print('[TOSS] 로그인 중...', flush=True)
    captured = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        async def on_req(req):
            if 'api-public.tossplace.com' in req.url and 'login' not in req.url:
                try:
                    h = await req.all_headers()
                except: return
                auth = h.get('authorization')
                wpid = h.get('toss-workplace-id')
                if auth and 'Bearer' in auth and wpid:
                    captured['headers'] = {
                        'Authorization': auth if auth.startswith('Bearer') else 'Bearer '+auth,
                        'toss-workplace-id': wpid,
                        'toss-place-user-id': h.get('toss-place-user-id', ''),
                        'Content-Type': 'application/json',
                        'User-Agent': h.get('user-agent', 'Mozilla/5.0'),
                    }
        page.on('request', on_req)

        await page.goto('https://dashboard.tossplace.com/login', wait_until='networkidle', timeout=30000)
        await page.wait_for_selector('input[autocomplete="username"]', timeout=10000)
        await page.fill('input[autocomplete="username"]', TOSS_ID)
        await page.fill('input[autocomplete="current-password"]', TOSS_PW)
        await page.click('button[type="submit"]')

        # 로그인 후 dashboard 진입 대기 + headers 캡쳐 대기
        for _ in range(30):
            await asyncio.sleep(1)
            if 'headers' in captured: break
        # 추가로 페이지 한번 navigate해서 headers 확실히 캡쳐
        if 'headers' not in captured:
            try:
                await page.goto('https://dashboard.tossplace.com/sales-detail/item-sale', wait_until='networkidle', timeout=15000)
                await asyncio.sleep(3)
            except: pass

        await browser.close()

    if 'headers' not in captured:
        raise RuntimeError('TOSS 로그인 후 헤더 캡쳐 실패')
    print(f'[TOSS] 완료 (workplace={captured["headers"]["toss-workplace-id"]})', flush=True)
    return captured['headers']


def toss_fetch_day(headers, date_str):
    body = {
        'merchantIds': [TOSS_MERCHANT_ID],
        'dateRange': {'start': date_str, 'end': date_str},
        'aggFields': ['ITEM_SALES'],
    }
    req = urllib.request.Request(
        f'{TOSS_BASE}/dashboard/v1/reports/multivariate/item-sales',
        data=json.dumps(body).encode(), headers=headers, method='POST')
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    items = data.get('success', {}).get('data', {}).get('itemSales', []) or []
    out = {}
    for it in items:
        nm = (it.get('itemTitle') or '').strip()
        c = it.get('content', {})
        qty = c.get('transactionCount', 0)
        net = c.get('amountMoney', 0)
        if nm and qty > 0:
            nm = normalize_name(nm)
            out[nm] = {'qty': qty, 'net': net}
    return out


# ── 월별 재생성 ────────────────────────────────────
def rebuild_month(year, month):
    yymm = f"{year%100:02d}{month:02d}"
    path = f'data/{yymm}.json'
    _, existing = gh_get(path)
    cat_map = {}
    if existing:
        for it in existing.get('items', []):
            nm = (it.get('item') or '').strip()
            if nm:
                cat_map[nm] = {k: it.get(k,'') or '' for k in ('cat_big','cat_mid','cat_small')}
    days = calendar.monthrange(year, month)[1]
    agg = {}
    for d in range(1, days+1):
        dt = date(year, month, d)
        if dt > date.today(): break
        fname = dt.strftime('%y%m%d') + '.json'
        _, daily = gh_get(f'data/daily/{fname}')
        if not daily: continue
        for store, items in daily.get('stores', {}).items():
            for it in items:
                nm = (it.get('item') or '').strip()
                if not nm: continue
                k = (store, nm)
                if k not in agg: agg[k] = {'qty':0,'net':0}
                agg[k]['qty'] += it.get('qty',0)
                agg[k]['net'] += it.get('net',0)
    items_out = []
    for (store, nm), v in agg.items():
        cat = cat_map.get(nm, {'cat_big':'','cat_mid':'','cat_small':''})
        items_out.append({'store':store,'item':nm,'qty':v['qty'],'net':v['net'],**cat})
    out = {'items': items_out}
    now = date.today()
    if year == now.year and month == now.month:
        out['note'] = '매일 밤 자동 최신화 (당일 포함, 전일까지 완전 반영)'
    elif existing and existing.get('note'):
        out['note'] = existing['note']
    content = json.dumps(out, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    sha = gh_exists(path)
    gh_put(path, content, f'auto: {yymm} 월별 재생성 ({len(items_out)}건)', sha=sha)
    print(f'[month] {yymm} 재생성 ({len(items_out)}건)', flush=True)


# ── 메인 ──────────────────────────────────────────
async def main():
    today = date.today()
    print(f'[start] {datetime.utcnow().isoformat()}Z UTC', flush=True)
    affected_months = set()

    # 1) OK POS
    okpos_session, csrf, savename = await okpos_login()
    include_today = True  # GH Actions는 정해진 시각에 돌므로 항상 당일 포함
    target_dates = []
    for i in range(0 if include_today else 1, LOOKBACK_DAYS+1):
        d = today - timedelta(days=i)
        fname = d.strftime('%y%m%d') + '.json'
        if d == today or not gh_exists(f'data/daily/{fname}'):
            target_dates.append(d)
    print(f'[OKPOS] 대상 날짜: {[d.isoformat() for d in target_dates]}', flush=True)

    for d in target_dates:
        date_str = d.strftime('%Y-%m-%d')
        fname = d.strftime('%y%m%d') + '.json'
        path = f'data/daily/{fname}'
        sha, existing = gh_get(path)
        day_stores = (existing or {}).get('stores', {}) if existing else {}
        for si in STORES.values():
            loc = si['location']
            try:
                rows = okpos_fetch_day(okpos_session, csrf, savename, date_str, si['code'], si['name'])
                bucket = day_stores.setdefault(loc, [])
                # 같은 매장 중복 방지: 기존 매장 키 항목 초기화하고 다시 채움
                # (지점 여러 코드(다산 1층/지하)는 합산)
                bucket_dict = {x['item']: x for x in bucket}
                for row in rows:
                    nm = (row.get('PROD_NM') or '').strip()
                    qty = int(row.get('SALE_QTY') or 0)
                    net = int(row.get('TOT_SALE_AMT') or 0)
                    if not is_valid(nm) or net == 0: continue
                    nm = normalize_name(nm)
                    if nm in bucket_dict:
                        bucket_dict[nm]['qty'] += qty
                        bucket_dict[nm]['net'] += net
                    else:
                        bucket_dict[nm] = {'item': nm, 'qty': qty, 'net': net}
                day_stores[loc] = list(bucket_dict.values())
            except Exception as e:
                print(f'  {date_str} {loc} 오류: {e}', flush=True)
        out = {'date': date_str, 'stores': day_stores}
        gh_put(path, json.dumps(out, ensure_ascii=False, separators=(',',':')).encode('utf-8'),
               f'auto: OKPOS {date_str}', sha=sha)
        affected_months.add((d.year, d.month))
        print(f'[OKPOS] {date_str} 업로드 ({sum(len(v) for v in day_stores.values())}건)', flush=True)

    # 2) TOSS 운정 (어제 + 오늘)
    try:
        toss_headers = await toss_login()
        toss_dates = [today, today - timedelta(days=1)]
        for d in toss_dates:
            try:
                items = toss_fetch_day(toss_headers, d.strftime('%Y-%m-%d'))
                if not items:
                    print(f'  TOSS {d.isoformat()}: 데이터 없음', flush=True)
                    continue
                fname = d.strftime('%y%m%d') + '.json'
                path = f'data/daily/{fname}'
                sha, existing = gh_get(path)
                out = existing or {'date': d.strftime('%Y-%m-%d'), 'stores': {}}
                out['stores']['운정'] = [{'item':nm,'qty':v['qty'],'net':v['net']} for nm,v in items.items()]
                gh_put(path, json.dumps(out, ensure_ascii=False, separators=(',',':')).encode('utf-8'),
                       f'auto: TOSS 운정 {d.isoformat()}', sha=sha)
                print(f'[TOSS] {d.isoformat()}: 운정 {len(items)}개', flush=True)
                affected_months.add((d.year, d.month))
            except Exception as e:
                print(f'  TOSS {d.isoformat()} 오류: {e}', flush=True)
    except Exception as e:
        print(f'[TOSS] 로그인/실행 실패: {e}', flush=True)

    # 3) 월별 재생성
    for y, m in affected_months:
        try:
            rebuild_month(y, m)
        except Exception as e:
            print(f'[month] {y}-{m} 재생성 실패: {e}', flush=True)

    print(f'[done] {datetime.utcnow().isoformat()}Z UTC', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
