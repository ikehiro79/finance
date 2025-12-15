import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timezone
from pathlib import Path

YF_QUOTE_URL = "https://query2.finance.yahoo.com/v7/finance/quote"

def normalize_symbol(s: str) -> str | None:
    s = (s or "").strip()
    if not s or s.startswith("#"):
        return None
    # 4桁だけなら東証(.T)として扱う（必要に応じて変更）
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

@st.cache_data(ttl=30)  # 30秒以内の再実行はYahooへ再問い合わせしない
def fetch_quotes(symbols: list[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()

    params = {"symbols": ",".join(symbols)}
    headers = {"User-Agent": "Mozilla/5.0"}  # 403回避のため付与（確実ではない）
    r = requests.get(YF_QUOTE_URL, params=params, headers=headers, timeout=15)
    r.raise_for_status()

    data = r.json()
    results = data.get("quoteResponse", {}).get("result", [])

    rows = []
    for q in results:
        ts = q.get("regularMarketTime")
        jst_time = None
        if ts:
            jst_time = (
                datetime.fromtimestamp(ts, tz=timezone.utc)
                .astimezone()  # 実行環境のTZ。必要ならAsia/Tokyoに固定してもOK
                .strftime("%Y-%m-%d %H:%M:%S")
            )

        rows.append({
            "symbol": q.get("symbol"),
            "name": q.get("shortName") or q.get("longName"),
            "price": q.get("regularMarketPrice"),
            "change": q.get("regularMarketChange"),
            "change_pct": q.get("regularMarketChangePercent"),
            "market_state": q.get("marketState"),
            "volume": q.get("regularMarketVolume"),
            "currency": q.get("currency"),
            "time": jst_time,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["symbol"]).reset_index(drop=True)
    return df

st.set_page_config(page_title="Yahoo 現在値ビューア", layout="wide")
st.title("Yahoo Finance 現在値（tickers.txt 読み込み）")

with st.sidebar:
    st.header("設定")
    ttl = st.number_input("キャッシュTTL（秒）", min_value=10, max_value=600, value=30, step=10)
    st.caption("短すぎるとレート制限を踏みやすくなります。")
    if st.button("今すぐ更新（キャッシュ破棄）"):
        st.cache_data.clear()

    st.divider()
    st.subheader("銘柄リスト")
    uploaded = st.file_uploader("tickers.txt をアップロード（任意）", type=["txt"])

# TTLを反映（簡易：ttl変更時はキャッシュをクリア）
# ※厳密にやるならfetch_quotesをラップする実装にします
if "last_ttl" not in st.session_state:
    st.session_state["last_ttl"] = ttl
elif st.session_state["last_ttl"] != ttl:
    st.cache_data.clear()
    st.session_state["last_ttl"] = ttl

# 銘柄リスト読み込み
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
st.write(f"読み込み銘柄数: **{len(symbols)}**")
st.code("\n".join(symbols[:50]) + ("\n..." if len(symbols) > 50 else ""))

# 取得＆表示
try:
    df = fetch_quotes(symbols)
    if df.empty:
        st.warning("データが空でした（銘柄コードやサフィックスをご確認ください）。")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
except requests.HTTPError as e:
    st.error(f"HTTPエラー: {e}")
    st.info("Community Cloudでは共有IPの影響でYahoo側にレート制限される場合があります。TTLを長めにしてください。")
except Exception as e:
    st.error(f"予期せぬエラー: {e}")
