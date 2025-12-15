import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

# =========================
# Yahoo Finance endpoints
# =========================
YF_BOOTSTRAP_URL = "https://finance.yahoo.com"  # cookie取得用
YF_GETCRUMB_URL  = "https://query2.finance.yahoo.com/v1/test/getcrumb"
YF_QUOTE_URL     = "https://query2.finance.yahoo.com/v7/finance/quote"

JST = ZoneInfo("Asia/Tokyo")


# =========================
# Utility
# =========================
def normalize_symbol(line: str) -> str | None:
    s = (line or "").strip()
    if not s or s.startswith("#"):
        return None
    # 4桁だけなら東証(.T)を自動付与（必要に応じてルール変更可）
    if s.isdigit() and len(s) == 4:
        return f"{s}.T"
    return s


def load_symbols_from_text(text: str) -> list[str]:
    symbols = []
    for line in (text or "").splitlines():
        sym = normalize_symbol(line)
        if sym:
            symbols.append(sym)
    # 重複排除＆ソート
    return sorted(set(symbols))


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fmt_jst_from_epoch(epoch: int | None) -> str | None:
    if not epoch:
        return None
    dt = datetime.fromtimestamp(epoch, tz=JST)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# =========================
# Yahoo session + crumb
# =========================
def get_yahoo_session() -> requests.Session:
    """
    1ユーザー(1セッション)ごとに requests.Session を持つ
    """
    if "yf_sess" not in st.session_state:
        s = requests.Session()
        # それっぽいブラウザヘッダを付与（弾かれにくくする）
        s.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer": "https://finance.yahoo.com/",
            "Connection": "keep-alive",
        })
        # まずトップへアクセスしてcookieを得る
        s.get(YF_BOOTSTRAP_URL, timeout=15)
        st.session_state["yf_sess"] = s
        st.session_state["yf_crumb"] = None
        st.session_state["yf_crumb_ts"] = 0.0

    return st.session_state["yf_sess"]


def get_crumb(sess: requests.Session, max_age_sec: int = 1800) -> str:
    """
    crumb は cookie と紐づくので、セッション内で一定時間キャッシュ
    """
    now = time.time()
    crumb = st.session_state.get("yf_crumb")
    ts = st.session_state.get("yf_crumb_ts", 0.0)

    if crumb and (now - ts) < max_age_sec:
        return crumb

    # 取得が空になることもあるので、最大2回試す
    for _ in range(2):
        r = sess.get(YF_GETCRUMB_URL, timeout=15)
        r.raise_for_status()
        crumb = (r.text or "").strip()
        if crumb:
            st.session_state["yf_crumb"] = crumb
            st.session_state["yf_crumb_ts"] = now
            return crumb

        # crumbが空ならbootstrapし直して再試行
        sess.get(YF_BOOTSTRAP_URL, timeout=15)

    raise RuntimeError("crumb の取得に失敗しました（Yahoo側でブロックされている可能性があります）")


def quote_once(sess: requests.Session, symbols: list[str]) -> list[dict]:
    """
    quote APIを1回叩く（symbolsは小分け済みを想定）
    401/403が出たら crumb/cookie を更新して1回だけ再試行
    """
    crumb = get_crumb(sess)
    params = {"symbols": ",".join(symbols), "crumb": crumb}
    r = sess.get(YF_QUOTE_URL, params=params, timeout=15)

    if r.status_code in (401, 403):
        # cookie/crumbリフレッシュしてリトライ
        st.session_state["yf_crumb"] = None
        sess.get(YF_BOOTSTRAP_URL, timeout=15)
        crumb = get_crumb(sess)
        params["crumb"] = crumb
        r = sess.get(YF_QUOTE_URL, params=params, timeout=15)

    r.raise_for_status()
    data = r.json()
    return data.get("quoteResponse", {}).get("result", [])


def build_df_from_results(results: list[dict], requested_symbols: list[str]) -> pd.DataFrame:
    rows = []
    got = set()

    for q in results:
        sym = q.get("symbol")
        got.add(sym)

        rows.append({
            "symbol": sym,
            "name": q.get("shortName") or q.get("longName"),
            "price": q.get("regularMarketPrice"),
            "change": q.get("regularMarketChange"),
            "change_pct": q.get("regularMarketChangePercent"),
            "market_state": q.get("marketState"),
            "volume": q.get("regularMarketVolume"),
            "currency": q.get("currency"),
            "time_jst": fmt_jst_from_epoch(q.get("regularMarketTime")),
        })

    # 取得できなかった銘柄があれば行を追加（原因特定しやすくする）
    missing = [s for s in requested_symbols if s not in got]
    for s in missing:
        rows.append({
            "symbol": s,
            "name": None,
            "price": None,
            "change": None,
            "change_pct": None,
            "market_state": None,
            "volume": None,
            "currency": None,
            "time_jst": None,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("symbol").reset_index(drop=True)
    return df


# =========================
# Caching (global)
# =========================
@st.cache_data(ttl=60, show_spinner=False)
def fetch_quotes_cached(symbols_tuple: tuple[str, ...], chunk_size: int) -> pd.DataFrame:
    """
    st.cache_data は（基本）全ユーザー共通キャッシュになります。
    共有IP環境では問い合わせ削減に効きます。
    """
    symbols = list(symbols_tuple)
    sess = get_yahoo_session()

    all_results: list[dict] = []
    for chunk in chunk_list(symbols, chunk_size):
        all_results.extend(quote_once(sess, chunk))

    return build_df_from_results(all_results, symbols)


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="Yahoo 現在値ビューア", layout="wide")
st.title("Yahoo Finance 現在値（tickers.txt 読み込み）")

with st.sidebar:
    st.header("設定")
    ttl = st.number_input("キャッシュTTL（秒）", min_value=10, max_value=600, value=60, step=10)
    chunk_size = st.number_input("一括取得の分割数（銘柄/リクエスト）", min_value=10, max_value=200, value=80, step=10)

    st.caption("共有IP環境では TTL を長め（60〜180秒）推奨です。")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("キャッシュ破棄", use_container_width=True):
            st.cache_data.clear()
    with col2:
        if st.button("cookie/crumb 再取得", use_container_width=True):
            st.session_state.pop("yf_sess", None)
            st.session_state.pop("yf_crumb", None)
            st.session_state.pop("yf_crumb_ts", None)

    st.divider()
    st.subheader("銘柄リスト")
    uploaded = st.file_uploader("tickers.txt をアップロード（任意）", type=["txt"])
    st.caption("形式: 1行1銘柄。4桁のみなら .T を自動付与。先頭 # はコメント。")

# TTLを変更したらキャッシュを破棄（ttlはデコレータ固定のため）
if "last_ttl" not in st.session_state:
    st.session_state["last_ttl"] = ttl
elif st.session_state["last_ttl"] != ttl:
    st.cache_data.clear()
    st.session_state["last_ttl"] = ttl

# tickers.txt 読み込み
tickers_text = None
if uploaded is not None:
    tickers_text = uploaded.getvalue().decode("utf-8", errors="ignore")
else:
    p = Path("tickers.txt")
    if p.exists():
        tickers_text = p.read_text(encoding="utf-8", errors="ignore")

if not tickers_text:
    st.warning("tickers.txt が見つかりません。サイドバーからアップロードするか、リポジトリ直下に配置してください。")
    st.stop()

symbols = load_symbols_from_text(tickers_text)

# 表示
st.write(f"読み込み銘柄数: **{len(symbols)}**")
with st.expander("読み込んだ銘柄一覧（先頭100件）", expanded=False):
    st.code("\n".join(symbols[:100]) + ("\n..." if len(symbols) > 100 else ""))

if not symbols:
    st.warning("銘柄が0件です。tickers.txt の内容を確認してください。")
    st.stop()

# 取得ボタン（自動で毎回取りに行くと弾かれやすいのでボタン式）
if "run_fetch" not in st.session_state:
    st.session_state["run_fetch"] = True

colA, colB = st.columns([1, 3])
with colA:
    do_fetch = st.button("現在値を取得", type="primary", use_container_width=True)
with colB:
    st.caption("ボタンを押したタイミングで取得します（リロード連打を避けるため）。")

if do_fetch:
    try:
        df = fetch_quotes_cached(tuple(symbols), int(chunk_size))
        st.subheader("結果")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # 簡易サマリ
        valid = df["price"].notna().sum()
        missing = df["price"].isna().sum()
        st.write(f"取得成功: **{valid}** / 失敗(空): **{missing}**")

        if missing:
            st.info("price が空の行は、銘柄コード（サフィックス含む）の誤り、またはYahoo側のブロックの可能性があります。")

    except requests.HTTPError as e:
        st.error(f"HTTPエラー: {e}")
        st.write("対策候補:")
        st.write("- キャッシュTTLを長くする（60〜180秒）")
        st.write("- 分割数（銘柄/リクエスト）を小さくする（例: 50）")
        st.write("- cookie/crumb 再取得ボタンを押してから再実行")
    except Exception as e:
        st.error(f"エラー: {e}")
        st.write("Community Cloud では共有IP等でYahoo側にブロックされる場合があります。時間を置く/TTLを伸ばす/データソース変更も検討してください。")
