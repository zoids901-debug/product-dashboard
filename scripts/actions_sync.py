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
import re

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


# ── 카테고리 보강: 누적 매핑표 + 키워드 fallback ──
import re as _re_cat
_MANUAL_CAT_MAP = None
def _load_manual_cat_map():
    """data/manual_cat_map.json — v2 형식 {global, byStore}. 1회 로드 후 캐시."""
    global _MANUAL_CAT_MAP
    if _MANUAL_CAT_MAP is not None:
        return _MANUAL_CAT_MAP
    try:
        _, content = gh_get('data/manual_cat_map.json')
    except Exception:
        content = None
    if not content:
        _MANUAL_CAT_MAP = {'global':{},'byStore':{}}
    else:
        # gh_get은 이미 parsed JSON(dict)을 반환 — 추가 파싱 불필요
        m = content if isinstance(content, dict) else json.loads(content)
        # v1(=평면 dict)는 global로 흡수
        if m.get('_format') == 'v2':
            _MANUAL_CAT_MAP = m
        else:
            _MANUAL_CAT_MAP = {'global': m, 'byStore': {}}
    return _MANUAL_CAT_MAP
def _norm_cat_key(s): return _re_cat.sub(r'[\s\W_]+','', s or '').lower()

# 키워드 fallback 룰 (운정 등 TOSS·카테고리 누락 메뉴용)
_FALLBACK_CAT_RULES = [
    (r'아메리카노|에스프레소|콜드브루|드립',                   ('음료','공통','커피')),
    (r'라떼|마끼아또|모카|카푸치노|플랫화이트',                ('음료','공통','커피')),
    (r'에이드',                                              ('음료','공통','에이드')),
    (r'블렌디드|프라페|스무디|쉐이크',                        ('음료','공통','논커피')),
    (r'^(?:.*\s)?티$|티(?:[\s]|$)|차(?:[\s]|$)|허브티|우롱', ('음료','공통','차')),
    (r'주스|쥬스',                                           ('음료','공통','논커피')),
    (r'밀크|초콜릿|쇼콜라|핫초코|코코아',                      ('음료','공통','논커피')),
    (r'케이크|타르트|마카롱|마들렌|휘낭시에|쿠키|브라우니|푸딩|크림빵', ('베이커리','디저트','디저트')),
    (r'크루아상|퀸아망|크라핀|페이스트리',                      ('베이커리','빵','크루아상류')),
    (r'소금빵',                                              ('베이커리','빵','소금빵')),
    (r'깜빠뉴|바게트|식빵|치아바타|포카치아|호밀빵|곡물빵',      ('베이커리','빵','식사빵')),
    (r'샌드위치|토스트|파니니|버거|핫도그',                     ('베이커리','샌드위치','샌드위치')),
    (r'베이글|스콘|머핀|도넛|크럼블|롤케익|롤케이크',            ('베이커리','빵','디저트빵')),
    (r'빵|번|롤|단팥|모카빵',                                 ('베이커리','빵','빵')),
    (r'증정|사은품|쿠폰|상품권',                              ('MD/기타','판촉','증정')),
    (r'리유저블백|에코백|머그|텀블러|컵|굿즈',                  ('MD/기타','MD','굿즈')),
    (r'원두|드립백|패키지',                                   ('MD/기타','MD','원두')),
]
def _fallback_cat(name):
    n = name or ''
    for pat, (b,m,s) in _FALLBACK_CAT_RULES:
        if _re_cat.search(pat, n):
            return {'cat_big':b,'cat_mid':m,'cat_small':s}
    return None

def _resolve_cat(nm, cat_map, daily_cat, store=None):
    """우선순위: 이번달 cat_map > 매장별 매핑 > daily_cat(OKPOS) > 글로벌 매핑(다수결) > 키워드 fallback > 빈값."""
    if nm in cat_map: return cat_map[nm]
    mm = _load_manual_cat_map()
    store_map = (mm.get('byStore') or {}).get(store or '', {}) if store else {}
    if nm in store_map: return store_map[nm]
    nk = _norm_cat_key(nm)
    if nk:
        for k,v in store_map.items():
            if _norm_cat_key(k) == nk: return v
    if daily_cat: return daily_cat
    gm = mm.get('global') or {}
    if nm in gm: return gm[nm]
    if nk:
        for k,v in gm.items():
            if _norm_cat_key(k) == nk: return v
    fb = _fallback_cat(nm)
    if fb: return fb
    return {'cat_big':'','cat_mid':'','cat_small':''}
# ── /카테고리 보강 끝 ──

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


# ── OK POS (순수 HTTP — 실시간 함수 prod-live.js 와 동일한 로그인 시퀀스) ──
OK_BASE = 'https://okasp.okpos.co.kr'
_CSRF_U = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

def _find_csrf(html):
    m = re.search(rf"name=['\"]({_CSRF_U})['\"]\s+value=['\"]({_CSRF_U})['\"]", html, re.I)
    if m: return m.group(1), m.group(2)
    m = re.search(rf"value=['\"]({_CSRF_U})['\"]\s+name=['\"]({_CSRF_U})['\"]", html, re.I)
    if m: return m.group(2), m.group(1)
    return None, None

async def okpos_login():
    """브라우저 없이 순수 HTTP 로그인 (okasp.okpos.co.kr 직접).
    반환: (requests.Session, {key,val} 세션 CSRF, savename='')."""
    print('[OKPOS] 로그인 중(HTTP)...', flush=True)
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko',
                      'Accept-Language': 'ko-KR'})
    ref = {'Referer': OK_BASE + '/login/login_form.jsp'}
    ck, cv = _find_csrf(s.get(OK_BASE + '/login/login_form.jsp', timeout=30).text)
    if not ck: raise RuntimeError('OKPOS 로그인 폼 CSRF 파싱 실패')
    cred = [('AutoFg', 'W'), ('user_id', OKPOS_ID), ('user_pwd', OKPOS_PW)]
    s.post(OK_BASE + '/login/login_check.jsp', data=[(ck, cv)] + cred, headers=ref, timeout=30)
    s.post(OK_BASE + '/login/login_check_action.jsp', data=[(ck, cv), (ck, cv)] + cred, headers=ref, timeout=30)
    sk = sv = None
    for p in ['/login/top_frame.jsp', '/login/top_page.jsp', '/login/history.jsp', '/login/showitem.jsp']:
        a, b = _find_csrf(s.get(OK_BASE + p, timeout=30).text)
        if a and not sk: sk, sv = a, b
    if not sk: raise RuntimeError('OKPOS 세션 CSRF 파싱 실패 (로그인 실패 가능)')
    # 판매(prod011) 폼 워밍업
    s.get(OK_BASE + '/sale/sale/prod010.jsp', timeout=30)
    s.get(OK_BASE + '/sale/sale/prod011.jsp', headers={'Referer': OK_BASE + '/sale/sale/prod010.jsp'}, timeout=30)
    print('[OKPOS] 완료(HTTP)', flush=True)
    return s, {'key': sk, 'val': sv}, ''

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
    """진짜 노이즈만 제외 — 특수문자(*,-,=) 포함된 진짜 상품(*레몬파운드, 인크*타바론 등)은 통과.
    1글자 상품명(X 등 OK포스 정식 등록 항목)도 통과 — 노이즈는 특수문자/공백만인 경우만."""
    if not name: return False
    stripped = name.replace('*','').replace('-','').replace('=','').replace(' ','').replace('★','').replace('☆','')
    if not stripped: return False  # 특수문자/공백만 → 노이즈
    if name.startswith('**') or name.startswith('--') or name.startswith('=='): return False
    return True


# 매장간 상품명 표기 차이 통일 (잘못된 표기 → OK포스 마스터 정식 표기)
NAME_ALIASES = {
    '애플 잼 스콘': '애플잼 스콘',
    # 운정(TOSS) 표기 → OK포스 인크 코드 마스터 표기 통일
    '카페라떼': 'I 카페 라떼',
    '카페모카': 'I 카페 모카',
    '피스타치오 퀸아망': '피스타치오 퀸 아망',
    '얼그레이 퀸아망': '얼 그레이 퀸 아망',
    '무화과크림치즈휘낭시에': '무화과 크림 치즈 휘낭시에',
    '얼그레이휘낭시에': '얼 그레이 휘낭시에',
    '햄치즈 소금빵': '햄 치즈 소금빵',
    '레몬 버터바': '레몬 버터 바',
    '피넛 버터바': '피넛 버터 바',
    '오렌지 쇼핑백': '오렌지쇼핑백',
    '다크 오리진 블렌드 (200g)': '다크 오리진블렌드 200g',
    '인크 오리진 블렌드 (200g)': '인크 오리진블렌드 200g',
    '벨벳 브리즈 (200g)': '벨벳브리즈 200g',
    '콜롬비아 디카페인 (200g)': '콜롬비아 디카페인(200g)',
    '인크 오리진 블렌드1KG': '인크 오리진블렌드 1Kg',
    '다크 오리진 블렌드 1KG': '다크 오리진블렌드 1Kg',
}

def normalize_name(name):
    return NAME_ALIASES.get(name, name)


# ── TOSS ──────────────────────────────────────────
def toss_login():
    """TOSS dashboard API 직접 호출로 accessToken + workspace_id 획득.
    브라우저 의존 제거 — playwright headless 캡쳐 실패/IP차단 우회."""
    print('[TOSS] 로그인 중 (API 직접)...', flush=True)
    body = {'id': TOSS_ID, 'password': TOSS_PW, 'loginType': 'DASHBOARD_USER'}
    req = urllib.request.Request(
        f'{TOSS_BASE}/api-public/dashboard/v2/auth/login',
        data=json.dumps(body).encode(),
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0',
            'Origin': 'https://dashboard.tossplace.com',
            'Accept': 'application/json',
        }, method='POST')
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    if data.get('resultType') != 'SUCCESS':
        raise RuntimeError(f"TOSS 로그인 실패: {data.get('error')}")
    token = data['success']['accessToken']
    # workspace id 조회 (type=BRAND)
    req = urllib.request.Request(
        f'{TOSS_BASE}/api-public/dashboard/v1/workspaces?type=BRAND',
        headers={
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Mozilla/5.0',
            'Origin': 'https://dashboard.tossplace.com',
            'Accept': 'application/json',
        })
    with urllib.request.urlopen(req, timeout=15) as r:
        ws = json.loads(r.read())
    items = (ws.get('success') or {}).get('items') or []
    if not items:
        raise RuntimeError("TOSS workspace(type=BRAND) 없음")
    wsid = items[0]['id']
    print(f'[TOSS] 완료 (workspace={wsid} {items[0].get("name","")})', flush=True)
    return {
        'Authorization': f'Bearer {token}',
        'dashboard-workspace-id': str(wsid),
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0',
        'Origin': 'https://dashboard.tossplace.com',
        'Accept': 'application/json',
    }


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
            # 카테고리 있는 entry만 cat_map에 등록 (빈 카테고리는 daily fallback 활용)
            if nm and it.get('cat_big'):
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
                if k not in agg: agg[k] = {'qty':0,'net':0,'daily_cat':None,'code':''}
                agg[k]['qty'] += it.get('qty',0)
                agg[k]['net'] += it.get('net',0)
                # 상품코드 — 첫 번째로 만난 값 유지
                if it.get('code') and not agg[k]['code']:
                    agg[k]['code'] = it['code']
                # daily에서 카테고리 학습 (OKPOS 응답 기반, 첫 번째 값 유지)
                if it.get('cat_big') and not agg[k]['daily_cat']:
                    agg[k]['daily_cat'] = {kk: it.get(kk,'') or '' for kk in ('cat_big','cat_mid','cat_small')}
    items_out = []
    for (store, nm), v in agg.items():
        # 우선순위: 기존 월별 cat_map (수동 보정 보존) > daily 학습 cat (OKPOS 자동) > 빈값
        cat = _resolve_cat(nm, cat_map, v.get('daily_cat'), store=store)
        items_out.append({'store':store,'item':nm,'code':v.get('code',''),'qty':v['qty'],'net':v['net'],**cat})
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


# ── manifest 재생성 ────────────────────────────────
def rebuild_manifest():
    """data/ 의 월별 파일(YYMM.json)을 스캔해 manifest.json을 최신화.
    새 달이 생기면 자동 등록 — 대시보드 조회기간 목록이 안 끊기도록.
    내용이 그대로면 commit 생략."""
    api = f'https://api.github.com/repos/{GH_REPO}/contents/data'
    with urllib.request.urlopen(urllib.request.Request(api, headers=GH_HEADERS), timeout=20) as r:
        entries = json.loads(r.read())
    months = []
    for e in entries:
        m = re.fullmatch(r'(\d{2})(\d{2})\.json', e.get('name', ''))
        if not m:
            continue
        yy, mm = int(m.group(1)), int(m.group(2))
        if not (1 <= mm <= 12):
            continue
        year = 2000 + yy
        months.append({'file': e['name'], 'label': f'{year}년 {mm}월', 'period': f'{year}-{mm:02d}'})
    months.sort(key=lambda x: x['file'])
    new_obj = {'months': months}
    sha, existing = gh_get('data/manifest.json')
    if existing == new_obj:
        print(f'[manifest] 변경 없음 ({len(months)}개월)', flush=True)
        return
    content = json.dumps(new_obj, ensure_ascii=False).encode('utf-8')
    gh_put('data/manifest.json', content, f'auto: manifest 재생성 ({len(months)}개월)', sha=sha)
    print(f'[manifest] 재생성 ({len(months)}개월, 마지막 {months[-1]["file"] if months else "-"})', flush=True)


# ── 메인 ──────────────────────────────────────────
async def main():
    today = date.today()
    print(f'[start] {datetime.utcnow().isoformat()}Z UTC', flush=True)
    affected_months = set()

    # 1) OK POS
    okpos_session, csrf, savename = await okpos_login()
    include_today = True  # GH Actions는 정해진 시각에 돌므로 항상 당일 포함
    # OKPOS가 채워야 하는 매장 location (운정은 TOSS 별도)
    # 하남/가산/다산은 2026-07-01 큐브포스 전환 → 로컬 수집기(cubepos_product.py)가 채우므로
    # 클라우드에선 조회/덮어쓰기 안 함(빈 OKPOS 결과로 지우면 안 됨).
    CUBE_LOCATIONS = {'하남', '가산', '다산'}
    OKPOS_LOCATIONS = {'수원', '광주'}
    target_dates = []
    for i in range(0 if include_today else 1, LOOKBACK_DAYS+1):
        d = today - timedelta(days=i)
        fname = d.strftime('%y%m%d') + '.json'
        needs_fetch = (d == today) or (not gh_exists(f'data/daily/{fname}'))
        if not needs_fetch:
            # 매장 누락 체크 — 5개 OKPOS 매장 모두 있고, 각 매장에 데이터 1건 이상 있어야
            # (빈 list[]도 "있음"으로 잘못 통과되던 버그 fix — 수원 1일 딜레이 시 발생)
            _, existing = gh_get(f'data/daily/{fname}')
            existing_stores = (existing or {}).get('stores', {})
            existing_locs = {loc for loc, items in existing_stores.items() if items}
            if not OKPOS_LOCATIONS.issubset(existing_locs):
                missing = OKPOS_LOCATIONS - existing_locs
                # 단, 오늘 sync 시점에서 어제 매출이 아직 안 들어왔을 수 있는 매장(수원)은
                # LOOKBACK 기간 내 재시도 자체에는 부담 없음 — 재수집 후에도 0이면 그냥 0
                print(f'[OKPOS] {d.isoformat()} 매출 0 또는 키 누락 매장: {missing} → 재수집', flush=True)
                needs_fetch = True
            else:
                # 카테고리 누락 체크 — 어느 매장이든 첫 item에 cat_big이 없으면 재수집
                for loc, items in (existing or {}).get('stores', {}).items():
                    if loc not in OKPOS_LOCATIONS: continue
                    if items and not items[0].get('cat_big'):
                        print(f'[OKPOS] {d.isoformat()} {loc} 카테고리 누락 → 재수집', flush=True)
                        needs_fetch = True
                        break
        if needs_fetch:
            target_dates.append(d)
    print(f'[OKPOS] 대상 날짜: {[d.isoformat() for d in target_dates]}', flush=True)

    for d in target_dates:
        date_str = d.strftime('%Y-%m-%d')
        fname = d.strftime('%y%m%d') + '.json'
        path = f'data/daily/{fname}'
        sha, existing = gh_get(path)
        day_stores = (existing or {}).get('stores', {}) if existing else {}
        # 재수집 시 같은 location에 누적되지 않도록, 이번 run에서 처음 만나는 매장은 bucket reset
        # (다산 1층 + 지하 같은 동일 loc 다중 SHOP_CD는 reset 이후 누적되어야 하므로 set으로 추적)
        fetched_locs = set()
        debug_log = []
        for si in STORES.values():
            loc = si['location']
            if loc in CUBE_LOCATIONS:
                continue  # 큐브포스 전환 매장 — 로컬 수집기가 채움(클라우드는 손대지 않음)
            try:
                rows = okpos_fetch_day(okpos_session, csrf, savename, date_str, si['code'], si['name'])
                sample = (rows[0] if rows else {})
                debug_log.append({
                    'date': date_str, 'code': si['code'], 'loc': loc,
                    'rows': len(rows),
                    'sample_keys': list(sample.keys())[:10] if sample else [],
                    'sample_prod': sample.get('PROD_NM', '') if sample else '',
                })
                print(f'  [{si["code"]}/{loc}] fetched {len(rows)} rows', flush=True)
                if loc not in fetched_locs:
                    day_stores[loc] = []  # 첫 fetch에서 기존 데이터 클리어
                    fetched_locs.add(loc)
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
                    code = (row.get('PROD_CD') or '').strip()
                    cat_big   = (row.get('LCLS_NM') or '').strip()
                    cat_mid   = (row.get('MCLS_NM') or '').strip()
                    cat_small = (row.get('SCLS_NM') or '').strip()
                    if nm in bucket_dict:
                        bucket_dict[nm]['qty'] += qty
                        bucket_dict[nm]['net'] += net
                        if not bucket_dict[nm].get('code') and code:
                            bucket_dict[nm]['code'] = code
                        # 카테고리는 첫 번째로 만난 값 유지 (덮어쓰지 않음)
                        if not bucket_dict[nm].get('cat_big') and cat_big:
                            bucket_dict[nm]['cat_big']   = cat_big
                            bucket_dict[nm]['cat_mid']   = cat_mid
                            bucket_dict[nm]['cat_small'] = cat_small
                    else:
                        bucket_dict[nm] = {'item': nm, 'code': code, 'qty': qty, 'net': net,
                                           'cat_big': cat_big, 'cat_mid': cat_mid, 'cat_small': cat_small}
                day_stores[loc] = list(bucket_dict.values())
            except Exception as e:
                print(f'  {date_str} {loc} 오류: {e}', flush=True)
        out = {'date': date_str, 'stores': day_stores}
        gh_put(path, json.dumps(out, ensure_ascii=False, separators=(',',':')).encode('utf-8'),
               f'auto: OKPOS {date_str}', sha=sha)
        # debug: 매장별 fetch 결과 저장
        dbg_path = f'data/_debug/fetch_{date_str}.json'
        dbg_sha = gh_exists(dbg_path)
        gh_put(dbg_path, json.dumps(debug_log, ensure_ascii=False, indent=2).encode('utf-8'),
               f'debug: fetch log {date_str}', sha=dbg_sha)
        affected_months.add((d.year, d.month))
        print(f'[OKPOS] {date_str} 업로드 ({sum(len(v) for v in day_stores.values())}건)', flush=True)

    # 2) TOSS 운정 (어제 + 오늘)
    try:
        toss_headers = toss_login()
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
                out['stores']['운정'] = [{'item':normalize_name(nm),'qty':v['qty'],'net':v['net']} for nm,v in items.items()]
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

    # 4) manifest 재생성 (새 달 자동 등록)
    try:
        rebuild_manifest()
    except Exception as e:
        print(f'[manifest] 재생성 실패: {e}', flush=True)

    print(f'[done] {datetime.utcnow().isoformat()}Z UTC', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
