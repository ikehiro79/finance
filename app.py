import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

YF_BOOTSTRAP_URL = "https://finance.yahoo.com"
YF_GETCRUMB_URL  = "https://query2.finance.yahoo.com/v1/test/getcrumb"
YF_QUOTE_URL     = "https://query2.finance.yahoo.com/v7/finance/quote"

JST = ZoneInfo("Asia/Tokyo")


def normalize_symbol(line: str) -> str | None:
    s = (line or "").strip()
    if not s or s.startswith("#"):
        return None
    if s.isdigit() and len(s) == 4:
        return f"{s}.T"
    return s


def load_symbols_from_text(text: str) -> list[str]:
    syms = []
    for line in (text or "").splitlines():
        sym = normalize_symbol(line)
        if sym:
            syms.append(sym)
    return sorted(set(syms))


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]


def fmt_jst_from_epoch(epoch: int | None) -> str | None:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, tz=JST).strftime("%Y-%m-%d %H:%M:%S")


class YahooRateLimitError(RuntimeError):
    pass


@dataclass
class YahooClient:
    session: requests.Session
    crumb: str | None = None
    crumb_ts: float = 0.0

    # アプリ全体で共有キャッシュ（IP共有環境で効く）
    last_df: pd.DataFrame | None = None
    last_fetch_ts: float = 0.0

    def _bootstrap(self) -> None:
        self.session.get(YF_BOOTSTRAP_URL, timeout=15)

    def _get_crumb(self, max_age_sec: int = 1800) -> str:
        now = time.time()
        if self.crumb and (now - self.crumb_ts) < max_age_sec:
            return self.crumb

        # 429対策：指数バックオフで最大3回だけ再試行
        backoffs = [1.0, 2.0, 4.0]
        for i, sleep_sec in enumerate([0.0] + backoffs):
            if sleep_sec > 0:
                time.sleep(sleep_sec)

            r = self.session.get(YF_GETCRUMB_URL, timeout=15)

            if r.status_code == 429:
                # 連打しない。次のバックオフへ
                continue

            r.raise_for_status()
            crumb = (r.text or "").strip()
            if crumb:
                self.crumb = crumb
                self.crumb_ts = time.time()
                return crumb

            # crumbが空なら cookie 取り直し
            self._bootstrap()

        raise YahooRateLimitError("Yahoo側のレート制限(429)により crumb を取得できませんでした。")

    def _quote_chunk(self, symbols: list[str]) -> list[dict]:
        """
        重要：最初は crumb無しで試す（= getcrumb の呼び出し回数を減らす）
        """
        params = {"symbols": ",".join(symbols)}
        r = self.session.get(YF_QUOTE_URL, params=params, timeout=15)

        if r.status_code == 429:
            raise YahooRateLimitError("Yahoo側のレート制限(429)により quote を取得できませんでした。")

        # 401/403 のときだけ crumb を取りに行く
        if r.status_code in (401, 403):
            crumb = self._get_crumb()
            params["crumb"] = crumb
            r = self.session.get(YF_QUOTE_URL, params=params, timeout=15)

        if r.status_code == 429:
            raise YahooRateLimitError("Yahoo側のレート制限(429)により quote を取得できませんでした。")

        r.raise_for_status()
        data = r.json()
        return data.get("quoteResponse", {}).get("result", [])

    def get_quotes_df(self, symbols: list[str], chunk_size: int) -> pd.DataFrame:
        all_results: list[dict] = []
        for c in chunk_list(symbols, chunk_size):
            all_results.extend(self._quote_chunk(c))

        rows = []
        got = set()
        for q in all_results:
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

        # 未取得銘柄の行（デバッグ用）
        missing = [s for s in symbols if s not in got]
        for s in missing:
            rows.append({"symbol": s})

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("symbol").reset_index(drop=True)
        return df

    def get_quotes_df_cached(self, symbols: list[str], chunk_size: int, ttl_sec: int, force: bool) -> pd.DataFrame:
        now = time.time()
        if (not force) and self.last_df is not None and (now - self.last_fetch_ts) < ttl_sec:
            return self.last_df

        df = self.get_quotes_df(symbols, chunk_size)
        self.last_df = df
        self.last_fetch_ts = now
        return df


@st.cache_resource
def get_client() -> YahooClient:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Referer": "https://finance.yahoo.com/",
        "Connection": "keep-alive",
    })
    client = YahooClient(session=s)
    client._bootstrap()
    return client


# =========================
# UI
# =========================
st.set_page_config(page_title="Yahoo 現在値ビューア", layout="wide")
st.title("Yahoo Finance 現在値（tickers.txt 読み込み）")

with st.sidebar:
    st.header("設定")
    ttl_sec = st.number_input("アプリ内キャッシュTTL（秒）", min_value=30, max_value=3600, value=180, step=30)
    chunk_size = st.number_input("一括取得の分割（銘柄/リクエスト）", min_value=10, max_value=200, value=60, step=10)

    st.divider()
    uploaded = st.file_uploader("tickers.txt をアップロード（任意）", type=["txt"])

    col1, col2 = st.columns(2)
    with col1:
        force = st.button("強制更新", use_container_width=True)
    with col2:
        reset = st.button("cookie/crumb リセット", use_container_width=True)

client = get_client()
if reset:
    # セッションを作り直すのが一番確実
    st.cache_resource.clear()
    st.rerun()

# tickers.txt 読み込み
tickers_text = None
if uploaded is not None:
    tickers_text = uploaded.getvalue().decode("utf-8", errors="ignore")
else:
    p = Path("tickers.txt")
    if p.exists():
        tickers_text = p.read_text(encoding="utf-8", errors="ignore")

if not tickers_text:
    st.warning("tickers.txt が見つかりません。アップロードするか、リポジトリ直下に配置してください。")
    st.stop()

symbols = load_symbols_from_text(tickers_text)
st.write(f"読み込み銘柄数: **{len(symbols)}**")
if not symbols:
    st.stop()

if st.button("現在値を取得", type="primary"):
    try:
        df = client.get_quotes_df_cached(symbols, int(chunk_size), int(ttl_sec), force=bool(force))
        st.dataframe(df, use_container_width=True, hide_index=True)

        ok = df["price"].notna().sum() if "price" in df.columns else 0
        st.write(f"取得成功: **{ok}** / 全件: **{len(df)}**")

    except YahooRateLimitError as e:
        st.error(str(e))
        st.info("対策：TTLを長くする（例 180〜300秒）、強制更新の多用を避ける、分割数を小さくする（例 40〜60）。")
    except requests.HTTPError as e:
        st.error(f"HTTPエラー: {e}")
    except Exception as e:
        st.error(f"エラー: {e}")
