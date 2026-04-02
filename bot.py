import os
import logging
import traceback
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import requests
import numpy as np
import pandas as pd
import yfinance as yf

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================================================
# 설정
# =========================================================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

if not TOKEN:
    raise ValueError("텔레그램 봇 토큰이 없습니다. 환경변수 TELEGRAM_BOT_TOKEN 을 설정하세요.")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("signal-bot")

BINANCE_BASE_URL = "https://api.binance.com"
REQUEST_TIMEOUT = 12
ASSETS_PER_PAGE = 12
FETCH_BARS = 400
DEFAULT_ANALYSIS_INTERVAL = "1m"
AUTO_INTERVALS = [30, 60, 120]

# 추가된 만기시간
EXPIRIES = [1, 2, 3, 5, 7, 10, 15, 30, 60, 240, 1440]

# =========================================================
# 종목 정의
# =========================================================
ACTUAL_FX: Dict[str, str] = {
    "AUD/CAD": "AUDCAD=X",
    "AUD/CHF": "AUDCHF=X",
    "AUD/JPY": "AUDJPY=X",
    "AUD/NZD": "AUDNZD=X",
    "AUD/USD": "AUDUSD=X",
    "CAD/CHF": "CADCHF=X",
    "CAD/JPY": "CADJPY=X",
    "CHF/JPY": "CHFJPY=X",
    "EUR/AUD": "EURAUD=X",
    "EUR/CAD": "EURCAD=X",
    "EUR/CHF": "EURCHF=X",
    "EUR/GBP": "EURGBP=X",
    "EUR/JPY": "EURJPY=X",
    "EUR/NZD": "EURNZD=X",
    "EUR/USD": "EURUSD=X",
    "GBP/AUD": "GBPAUD=X",
    "GBP/CAD": "GBPCAD=X",
    "GBP/CHF": "GBPCHF=X",
    "GBP/JPY": "GBPJPY=X",
    "GBP/NZD": "GBPNZD=X",
    "GBP/USD": "GBPUSD=X",
    "NZD/CAD": "NZDCAD=X",
    "NZD/CHF": "NZDCHF=X",
    "NZD/JPY": "NZDJPY=X",
    "NZD/USD": "NZDUSD=X",
    "USD/CAD": "USDCAD=X",
    "USD/CHF": "USDCHF=X",
    "USD/JPY": "USDJPY=X",
}

OTC_PROXY: Dict[str, str] = {
    "AUD/CAD OTC": "AUDCAD=X",
    "AUD/CHF OTC": "AUDCHF=X",
    "AUD/JPY OTC": "AUDJPY=X",
    "AUD/NZD OTC": "AUDNZD=X",
    "AUD/USD OTC": "AUDUSD=X",
    "CAD/CHF OTC": "CADCHF=X",
    "CAD/JPY OTC": "CADJPY=X",
    "CHF/JPY OTC": "CHFJPY=X",
    "EUR/AUD OTC": "EURAUD=X",
    "EUR/CAD OTC": "EURCAD=X",
    "EUR/CHF OTC": "EURCHF=X",
    "EUR/GBP OTC": "EURGBP=X",
    "EUR/JPY OTC": "EURJPY=X",
    "EUR/NZD OTC": "EURNZD=X",
    "EUR/USD OTC": "EURUSD=X",
    "GBP/AUD OTC": "GBPAUD=X",
    "GBP/CAD OTC": "GBPCAD=X",
    "GBP/CHF OTC": "GBPCHF=X",
    "GBP/JPY OTC": "GBPJPY=X",
    "GBP/NZD OTC": "GBPNZD=X",
    "GBP/USD OTC": "GBPUSD=X",
    "NZD/CAD OTC": "NZDCAD=X",
    "NZD/CHF OTC": "NZDCHF=X",
    "NZD/JPY OTC": "NZDJPY=X",
    "NZD/USD OTC": "NZDUSD=X",
    "USD/CAD OTC": "USDCAD=X",
    "USD/CHF OTC": "USDCHF=X",
    "USD/JPY OTC": "USDJPY=X",
}

CRYPTO: Dict[str, str] = {
    "ADA/USDT": "ADAUSDT",
    "APT/USDT": "APTUSDT",
    "ARB/USDT": "ARBUSDT",
    "ATOM/USDT": "ATOMUSDT",
    "AVAX/USDT": "AVAXUSDT",
    "BCH/USDT": "BCHUSDT",
    "BNB/USDT": "BNBUSDT",
    "BTC/USDT": "BTCUSDT",
    "DOGE/USDT": "DOGEUSDT",
    "DOT/USDT": "DOTUSDT",
    "ETH/USDT": "ETHUSDT",
    "FIL/USDT": "FILUSDT",
    "INJ/USDT": "INJUSDT",
    "LINK/USDT": "LINKUSDT",
    "LTC/USDT": "LTCUSDT",
    "MATIC/USDT": "MATICUSDT",
    "NEAR/USDT": "NEARUSDT",
    "OP/USDT": "OPUSDT",
    "PEPE/USDT": "PEPEUSDT",
    "SAND/USDT": "SANDUSDT",
    "SEI/USDT": "SEIUSDT",
    "SOL/USDT": "SOLUSDT",
    "TIA/USDT": "TIAUSDT",
    "TON/USDT": "TONUSDT",
    "TRX/USDT": "TRXUSDT",
    "UNI/USDT": "UNIUSDT",
    "WIF/USDT": "WIFUSDT",
    "XRP/USDT": "XRPUSDT",
}

STRATEGIES = {
    "s1": "1번 전략 · 최신 컨플루언스",
    "s2": "2번 전략 · 더블BB/더블돈치안/더블PSAR/트리플EMA/더블MACD",
    "s3": "3번 전략 · 더블Stoch/더블RSI/트리플EMA/돈치안",
}

MARKETS = {
    "fx": "실제화폐",
    "otc": "OTC",
    "crypto": "암호화폐",
}

# =========================================================
# 데이터 클래스
# =========================================================
@dataclass
class SignalResult:
    expiry_signals: Dict[int, Tuple[str, float]]
    final_signal: str
    final_score: float
    confidence: int
    reason_lines: List[str]
    price: float
    market_note: str
    recommended_expiry: Optional[int]


# =========================================================
# 기본 유틸
# =========================================================
def sort_assets(d: Dict[str, str]) -> List[Tuple[str, str]]:
    return sorted(d.items(), key=lambda x: x[0].upper())


def safe_float(v, default=0.0):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def normalize_score(v: float, lo: float = -10.0, hi: float = 10.0) -> float:
    if hi == lo:
        return 0.0
    x = (v - lo) / (hi - lo)
    x = clamp(x, 0.0, 1.0)
    return x * 2 - 1


def signal_from_score(score: float, threshold: float = 0.12) -> str:
    if score >= threshold:
        return "LONG"
    if score <= -threshold:
        return "SHORT"
    return "관망"


def format_expiry_label(minutes: int) -> str:
    if minutes == 60:
        return "1시간"
    if minutes == 240:
        return "4시간"
    if minutes == 1440:
        return "1일"
    return f"{minutes}분"


def get_assets_by_market(market: str) -> List[Tuple[str, str]]:
    if market == "fx":
        return sort_assets(ACTUAL_FX)
    if market == "otc":
        return sort_assets(OTC_PROXY)
    if market == "crypto":
        return sort_assets(CRYPTO)
    return []


def get_asset_code(market: str, asset_name: str) -> Optional[str]:
    items = dict(get_assets_by_market(market))
    return items.get(asset_name)


# =========================================================
# 데이터 로딩
# =========================================================
def fetch_binance_klines(symbol: str, interval: str = "1m", limit: int = FETCH_BARS) -> pd.DataFrame:
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["time", "open", "high", "low", "close", "volume"]].dropna().reset_index(drop=True)
    return df


def fetch_yfinance_1m(symbol: str) -> pd.DataFrame:
    df = yf.download(
        tickers=symbol,
        period="2d",
        interval="1m",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        raise ValueError(f"Yahoo Finance 데이터 없음: {symbol}")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" not in df.columns:
        df["volume"] = 1.0
    else:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

    df = df.reset_index()
    time_col = "Datetime" if "Datetime" in df.columns else "Date"
    df["time"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    df = df[["time", "open", "high", "low", "close", "volume"]].dropna().reset_index(drop=True)

    if len(df) > FETCH_BARS:
        df = df.tail(FETCH_BARS).reset_index(drop=True)

    return df


def fetch_market_data(market: str, symbol_code: str) -> Tuple[pd.DataFrame, str]:
    if market == "crypto":
        return fetch_binance_klines(symbol_code, interval=DEFAULT_ANALYSIS_INTERVAL), "Binance 현물 1분봉"
    if market == "fx":
        return fetch_yfinance_1m(symbol_code), "Yahoo Finance 실제 FX 1분봉"
    if market == "otc":
        return fetch_yfinance_1m(symbol_code), "OTC 프록시 분석 (대응 실제 FX 1분봉)"
    raise ValueError("알 수 없는 시장 유형")


# =========================================================
# 지표 함수
# =========================================================
def ema(s: pd.Series, length: int) -> pd.Series:
    return s.ewm(span=length, adjust=False).mean()


def sma(s: pd.Series, length: int) -> pd.Series:
    return s.rolling(length).mean()


def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def rsi(s: pd.Series, length: int = 14) -> pd.Series:
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1 / length, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(s: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    fast_ema = ema(s, fast)
    slow_ema = ema(s, slow)
    line = fast_ema - slow_ema
    sig = ema(line, signal)
    hist = line - sig
    return line, sig, hist


def stochastic(df: pd.DataFrame, k_len: int = 14, d_len: int = 3, smooth_k: int = 3):
    low_min = df["low"].rolling(k_len).min()
    high_max = df["high"].rolling(k_len).max()
    k = 100 * (df["close"] - low_min) / (high_max - low_min).replace(0, np.nan)
    k = k.rolling(smooth_k).mean()
    d = k.rolling(d_len).mean()
    return k, d


def bollinger(s: pd.Series, length: int = 20, std_mult: float = 2.0):
    basis = sma(s, length)
    dev = s.rolling(length).std()
    upper = basis + dev * std_mult
    lower = basis - dev * std_mult
    width = (upper - lower) / basis.replace(0, np.nan)
    return basis, upper, lower, width


def donchian(df: pd.DataFrame, length: int = 20):
    upper = df["high"].rolling(length).max()
    lower = df["low"].rolling(length).min()
    mid = (upper + lower) / 2
    return upper, lower, mid


def adx(df: pd.DataFrame, length: int = 14):
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = pd.concat([
        (high - low).abs(),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)

    atr_s = tr.ewm(alpha=1 / length, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / length, adjust=False).mean() / atr_s.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / length, adjust=False).mean() / atr_s.replace(0, np.nan)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx_line = dx.ewm(alpha=1 / length, adjust=False).mean()
    return plus_di, minus_di, adx_line


def psar(df: pd.DataFrame, af_step: float = 0.02, af_max: float = 0.2) -> pd.Series:
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 3:
        return pd.Series(np.nan, index=df.index)

    psar_vals = np.zeros(n)
    bull = True
    af = af_step
    ep = high[0]
    psar_vals[0] = low[0]
    psar_vals[1] = low[0]

    for i in range(2, n):
        prev_psar = psar_vals[i - 1]

        if bull:
            psar_vals[i] = prev_psar + af * (ep - prev_psar)
            psar_vals[i] = min(psar_vals[i], low[i - 1], low[i - 2])

            if low[i] < psar_vals[i]:
                bull = False
                psar_vals[i] = ep
                ep = low[i]
                af = af_step
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_step, af_max)
        else:
            psar_vals[i] = prev_psar + af * (ep - prev_psar)
            psar_vals[i] = max(psar_vals[i], high[i - 1], high[i - 2])

            if high[i] > psar_vals[i]:
                bull = True
                psar_vals[i] = ep
                ep = high[i]
                af = af_step
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + af_step, af_max)

    return pd.Series(psar_vals, index=df.index)


def rolling_vwap(df: pd.DataFrame, length: int = 50) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].copy().replace(0, 1.0).fillna(1.0)
    pv = typical * vol
    return pv.rolling(length).sum() / vol.rolling(length).sum().replace(0, np.nan)


def supertrend(df: pd.DataFrame, length: int = 10, multiplier: float = 3.0):
    hl2 = (df["high"] + df["low"]) / 2.0
    atr_val = atr(df, length)
    upperband = hl2 + multiplier * atr_val
    lowerband = hl2 - multiplier * atr_val

    final_upperband = upperband.copy()
    final_lowerband = lowerband.copy()

    for i in range(1, len(df)):
        if df["close"].iloc[i - 1] <= final_upperband.iloc[i - 1]:
            final_upperband.iloc[i] = min(upperband.iloc[i], final_upperband.iloc[i - 1])
        else:
            final_upperband.iloc[i] = upperband.iloc[i]

        if df["close"].iloc[i - 1] >= final_lowerband.iloc[i - 1]:
            final_lowerband.iloc[i] = max(lowerband.iloc[i], final_lowerband.iloc[i - 1])
        else:
            final_lowerband.iloc[i] = lowerband.iloc[i]

    trend = pd.Series(index=df.index, dtype=float)
    st = pd.Series(index=df.index, dtype=float)

    trend.iloc[0] = 1
    st.iloc[0] = final_lowerband.iloc[0]

    for i in range(1, len(df)):
        prev_trend = trend.iloc[i - 1]
        if prev_trend == 1:
            if df["close"].iloc[i] < final_lowerband.iloc[i]:
                trend.iloc[i] = -1
                st.iloc[i] = final_upperband.iloc[i]
            else:
                trend.iloc[i] = 1
                st.iloc[i] = final_lowerband.iloc[i]
        else:
            if df["close"].iloc[i] > final_upperband.iloc[i]:
                trend.iloc[i] = 1
                st.iloc[i] = final_lowerband.iloc[i]
            else:
                trend.iloc[i] = -1
                st.iloc[i] = final_upperband.iloc[i]

    return st, trend


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ema_8"] = ema(df["close"], 8)
    df["ema_9"] = ema(df["close"], 9)
    df["ema_12"] = ema(df["close"], 12)
    df["ema_20"] = ema(df["close"], 20)
    df["ema_21"] = ema(df["close"], 21)
    df["ema_34"] = ema(df["close"], 34)
    df["ema_50"] = ema(df["close"], 50)
    df["ema_55"] = ema(df["close"], 55)

    df["rsi_7"] = rsi(df["close"], 7)
    df["rsi_14"] = rsi(df["close"], 14)

    df["macd_12"], df["macd_signal_12"], df["macd_hist_12"] = macd(df["close"], 12, 26, 9)
    df["macd_8"], df["macd_signal_8"], df["macd_hist_8"] = macd(df["close"], 8, 21, 5)

    df["stoch_k_5"], df["stoch_d_5"] = stochastic(df, 5, 3, 2)
    df["stoch_k_14"], df["stoch_d_14"] = stochastic(df, 14, 3, 3)

    df["bb_basis_20"], df["bb_upper_20"], df["bb_lower_20"], df["bb_width_20"] = bollinger(df["close"], 20, 2.0)
    df["bb_basis_50"], df["bb_upper_50"], df["bb_lower_50"], df["bb_width_50"] = bollinger(df["close"], 50, 2.0)

    df["dc_upper_10"], df["dc_lower_10"], df["dc_mid_10"] = donchian(df, 10)
    df["dc_upper_20"], df["dc_lower_20"], df["dc_mid_20"] = donchian(df, 20)
    df["dc_upper_55"], df["dc_lower_55"], df["dc_mid_55"] = donchian(df, 55)

    df["atr_14"] = atr(df, 14)
    df["plus_di"], df["minus_di"], df["adx_14"] = adx(df, 14)

    df["psar_fast"] = psar(df, 0.02, 0.2)
    df["psar_slow"] = psar(df, 0.01, 0.1)

    df["vwap_50"] = rolling_vwap(df, 50)

    df["st_line"], df["st_trend"] = supertrend(df, 10, 3.0)

    df["kc_mid_20"] = ema(df["close"], 20)
    df["kc_upper_20"] = df["kc_mid_20"] + df["atr_14"] * 1.5
    df["kc_lower_20"] = df["kc_mid_20"] - df["atr_14"] * 1.5

    df["ret_1"] = df["close"].pct_change()
    df["mom_3"] = df["close"].pct_change(3)
    df["mom_5"] = df["close"].pct_change(5)
    df["mom_10"] = df["close"].pct_change(10)
    df["mom_20"] = df["close"].pct_change(20)
    df["ema20_slope"] = df["ema_20"].diff(3)
    df["ema50_slope"] = df["ema_50"].diff(3)
    df["macd_hist_12_slope"] = df["macd_hist_12"].diff(2)
    df["macd_hist_8_slope"] = df["macd_hist_8"].diff(2)

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna().reset_index(drop=True)

    if df.empty:
        raise ValueError("지표 계산 후 데이터가 비었습니다.")

    return df


# =========================================================
# 전략 로직
# =========================================================
def build_strategy_1(df: pd.DataFrame) -> Tuple[float, List[str]]:
    row = df.iloc[-1]
    prev = df.iloc[-2]

    score = 0.0
    reasons = []

    if row["st_trend"] > 0:
        score += 1.8
        reasons.append("• Supertrend 상승")
    else:
        score -= 1.8
        reasons.append("• Supertrend 하락")

    if row["ema_20"] > row["ema_50"] and row["close"] > row["ema_20"]:
        score += 1.6
        reasons.append("• EMA20 > EMA50, 가격이 EMA20 위")
    elif row["ema_20"] < row["ema_50"] and row["close"] < row["ema_20"]:
        score -= 1.6
        reasons.append("• EMA20 < EMA50, 가격이 EMA20 아래")

    if row["macd_hist_12"] > 0 and row["macd_hist_12_slope"] > 0:
        score += 1.4
        reasons.append("• MACD 히스토그램 상승 확장")
    elif row["macd_hist_12"] < 0 and row["macd_hist_12_slope"] < 0:
        score -= 1.4
        reasons.append("• MACD 히스토그램 하락 확장")

    if 52 <= row["rsi_14"] <= 68:
        score += 0.9
        reasons.append("• RSI14 상승 우위 구간")
    elif 32 <= row["rsi_14"] <= 48:
        score -= 0.9
        reasons.append("• RSI14 하락 우위 구간")

    if row["adx_14"] >= 18:
        if row["plus_di"] > row["minus_di"]:
            score += 1.1
            reasons.append("• ADX 추세 강도 +DI 우세")
        elif row["minus_di"] > row["plus_di"]:
            score -= 1.1
            reasons.append("• ADX 추세 강도 -DI 우세")

    if row["close"] > row["vwap_50"]:
        score += 0.7
        reasons.append("• 가격이 VWAP50 위")
    else:
        score -= 0.7
        reasons.append("• 가격이 VWAP50 아래")

    squeeze_on = (row["bb_upper_20"] < row["kc_upper_20"]) and (row["bb_lower_20"] > row["kc_lower_20"])
    squeeze_prev = (prev["bb_upper_20"] < prev["kc_upper_20"]) and (prev["bb_lower_20"] > prev["kc_lower_20"])

    if squeeze_prev and not squeeze_on:
        if row["close"] > row["bb_basis_20"]:
            score += 1.2
            reasons.append("• BB/KC squeeze 해제 후 상방")
        elif row["close"] < row["bb_basis_20"]:
            score -= 1.2
            reasons.append("• BB/KC squeeze 해제 후 하방")

    return score, reasons


def build_strategy_2(df: pd.DataFrame) -> Tuple[float, List[str]]:
    row = df.iloc[-1]
    score = 0.0
    reasons = []

    long_bb = row["close"] > row["bb_basis_20"] and row["close"] > row["bb_basis_50"]
    short_bb = row["close"] < row["bb_basis_20"] and row["close"] < row["bb_basis_50"]
    if long_bb:
        score += 1.5
        reasons.append("• 더블 BB 상단 구조")
    elif short_bb:
        score -= 1.5
        reasons.append("• 더블 BB 하단 구조")

    if row["close"] > row["dc_mid_10"] and row["close"] > row["dc_mid_20"]:
        score += 1.4
        reasons.append("• 더블 돈치안 중앙선 위")
    elif row["close"] < row["dc_mid_10"] and row["close"] < row["dc_mid_20"]:
        score -= 1.4
        reasons.append("• 더블 돈치안 중앙선 아래")

    long_psar = row["close"] > row["psar_fast"] and row["close"] > row["psar_slow"]
    short_psar = row["close"] < row["psar_fast"] and row["close"] < row["psar_slow"]
    if long_psar:
        score += 1.5
        reasons.append("• 더블 PSAR 상승")
    elif short_psar:
        score -= 1.5
        reasons.append("• 더블 PSAR 하락")

    if row["ema_9"] > row["ema_21"] > row["ema_55"]:
        score += 1.8
        reasons.append("• 트리플 EMA 상승 정렬")
    elif row["ema_9"] < row["ema_21"] < row["ema_55"]:
        score -= 1.8
        reasons.append("• 트리플 EMA 하락 정렬")

    long_macd = row["macd_hist_12"] > 0 and row["macd_hist_8"] > 0
    short_macd = row["macd_hist_12"] < 0 and row["macd_hist_8"] < 0
    if long_macd:
        score += 1.4
        reasons.append("• 더블 MACD 양수")
    elif short_macd:
        score -= 1.4
        reasons.append("• 더블 MACD 음수")

    return score, reasons


def build_strategy_3(df: pd.DataFrame) -> Tuple[float, List[str]]:
    row = df.iloc[-1]
    prev = df.iloc[-2]
    score = 0.0
    reasons = []

    long_stoch = row["stoch_k_5"] > row["stoch_d_5"] and row["stoch_k_14"] > row["stoch_d_14"]
    short_stoch = row["stoch_k_5"] < row["stoch_d_5"] and row["stoch_k_14"] < row["stoch_d_14"]

    if long_stoch:
        score += 1.6
        reasons.append("• 더블 스토캐스틱 상승")
    elif short_stoch:
        score -= 1.6
        reasons.append("• 더블 스토캐스틱 하락")

    if row["rsi_7"] > 52 and row["rsi_14"] > 50:
        score += 1.5
        reasons.append("• 더블 RSI 상승 우위")
    elif row["rsi_7"] < 48 and row["rsi_14"] < 50:
        score -= 1.5
        reasons.append("• 더블 RSI 하락 우위")

    if row["ema_8"] > row["ema_21"] > row["ema_55"]:
        score += 1.7
        reasons.append("• 트리플 EMA 상승 정렬")
    elif row["ema_8"] < row["ema_21"] < row["ema_55"]:
        score -= 1.7
        reasons.append("• 트리플 EMA 하락 정렬")

    if row["close"] > row["dc_mid_20"]:
        score += 0.9
        reasons.append("• 돈치안20 중앙선 위")
    else:
        score -= 0.9
        reasons.append("• 돈치안20 중앙선 아래")

    if row["close"] > prev["close"] and row["rsi_7"] > prev["rsi_7"]:
        score += 0.5
    elif row["close"] < prev["close"] and row["rsi_7"] < prev["rsi_7"]:
        score -= 0.5

    return score, reasons


# =========================================================
# 만기시간 분석
# =========================================================
def compute_horizon_score(df: pd.DataFrame, base_score: float, expiry_min: int) -> float:
    row = df.iloc[-1]

    atr_pct = safe_float(row["atr_14"] / row["close"], 0.0)
    ema_slope = safe_float(row["ema20_slope"] / row["close"], 0.0)
    macd_slope = safe_float(row["macd_hist_12_slope"], 0.0)
    mom3 = safe_float(row["mom_3"], 0.0)
    mom5 = safe_float(row["mom_5"], 0.0)
    mom10 = safe_float(row["mom_10"], 0.0)
    mom20 = safe_float(row["mom_20"], 0.0)
    adx_val = safe_float(row["adx_14"], 0.0)

    if expiry_min <= 5:
        trend_weight = 0.45
        macd_weight = 1.15
        mom_weight = (mom3 * 100) * 0.9 + (mom5 * 100) * 0.5
        noise_penalty = 0.35 if atr_pct > 0.0035 else 0.0
    elif expiry_min <= 15:
        trend_weight = 0.65
        macd_weight = 1.20
        mom_weight = (mom3 * 100) * 0.6 + (mom5 * 100) * 0.8 + (mom10 * 100) * 0.3
        noise_penalty = 0.22 if atr_pct > 0.0045 else 0.0
    elif expiry_min <= 60:
        trend_weight = 0.95
        macd_weight = 1.35
        mom_weight = (mom5 * 100) * 0.6 + (mom10 * 100) * 0.8 + (mom20 * 100) * 0.35
        noise_penalty = 0.10 if atr_pct > 0.006 else 0.0
    else:
        trend_weight = 1.25
        macd_weight = 1.45
        mom_weight = (mom10 * 100) * 0.6 + (mom20 * 100) * 0.9
        noise_penalty = 0.0

    trend_boost = (ema_slope * 1500) * trend_weight
    macd_boost = macd_slope * macd_weight
    adx_boost = 0.0

    if adx_val >= 18:
        adx_boost = (adx_val - 18) * 0.03
        if base_score < 0:
            adx_boost *= -1

    raw = (base_score * 0.72) + trend_boost + macd_boost + mom_weight + adx_boost

    if raw > 0:
        raw -= noise_penalty
    elif raw < 0:
        raw += noise_penalty

    return raw


def choose_recommended_expiry(expiry_signals: Dict[int, Tuple[str, float]], final_signal: str) -> Optional[int]:
    if final_signal == "관망":
        return None

    best_expiry = None
    best_strength = -1.0

    for expiry, (sig, score) in expiry_signals.items():
        if sig != final_signal:
            continue

        strength = abs(score)

        if expiry in (7, 15, 30, 60):
            strength += 0.03
        elif expiry in (240, 1440):
            strength += 0.01

        if strength > best_strength:
            best_strength = strength
            best_expiry = expiry

    return best_expiry


def analyze_symbol(df: pd.DataFrame, strategy_key: str, market_note: str) -> SignalResult:
    if len(df) < 120:
        raise ValueError("지표 계산에 필요한 데이터가 부족합니다.")

    if strategy_key == "s1":
        base_score, reasons = build_strategy_1(df)
    elif strategy_key == "s2":
        base_score, reasons = build_strategy_2(df)
    elif strategy_key == "s3":
        base_score, reasons = build_strategy_3(df)
    else:
        raise ValueError("알 수 없는 전략입니다.")

    expiry_signals = {}
    all_scores = []

    for e in EXPIRIES:
        s = compute_horizon_score(df, base_score, e)
        norm = normalize_score(s, -8.0, 8.0)

        if e <= 2:
            threshold = 0.14
        elif e <= 10:
            threshold = 0.12
        elif e <= 60:
            threshold = 0.11
        else:
            threshold = 0.10

        sig = signal_from_score(norm, threshold=threshold)
        expiry_signals[e] = (sig, norm)
        all_scores.append((e, norm))

    weights = {
        1: 0.08, 2: 0.08, 3: 0.08, 5: 0.08, 7: 0.08,
        10: 0.10, 15: 0.10, 30: 0.11, 60: 0.11, 240: 0.09, 1440: 0.09,
    }

    weighted = sum(expiry_signals[e][1] * weights[e] for e in EXPIRIES)

    long_count = sum(1 for e in EXPIRIES if expiry_signals[e][0] == "LONG")
    short_count = sum(1 for e in EXPIRIES if expiry_signals[e][0] == "SHORT")

    if weighted >= 0.18 and long_count >= 4:
        final_signal = "LONG"
    elif weighted <= -0.18 and short_count >= 4:
        final_signal = "SHORT"
    else:
        final_signal = "관망"

    confidence = int(clamp(abs(weighted) * 100, 35, 95))
    price = safe_float(df["close"].iloc[-1], 0.0)
    recommended_expiry = choose_recommended_expiry(expiry_signals, final_signal)

    return SignalResult(
        expiry_signals=expiry_signals,
        final_signal=final_signal,
        final_score=weighted,
        confidence=confidence,
        reason_lines=reasons[:8],
        price=price,
        market_note=market_note,
        recommended_expiry=recommended_expiry,
    )


# =========================================================
# 텍스트 포맷
# =========================================================
def format_signal_message(user_state: dict, result: SignalResult) -> str:
    strategy_name = STRATEGIES.get(user_state.get("strategy", ""), "미선택")
    market_name = MARKETS.get(user_state.get("market", ""), "미선택")
    asset_name = user_state.get("asset_name", "미선택")

    lines = []
    lines.append("📊 시그널 분석 결과")
    lines.append("")
    lines.append(f"전략: {strategy_name}")
    lines.append(f"시장: {market_name}")
    lines.append(f"종목: {asset_name}")
    lines.append(f"현재가: {result.price:.6f}")
    lines.append(f"데이터: {result.market_note}")
    lines.append("")
    lines.append("⏱ 만기별 결과")

    for e in EXPIRIES:
        sig, sc = result.expiry_signals[e]
        arrow = "🟢" if sig == "LONG" else ("🔴" if sig == "SHORT" else "🟡")
        lines.append(f"{arrow} {format_expiry_label(e)}: {sig}  (점수 {sc:+.2f})")

    lines.append("")
    final_emoji = "🟢" if result.final_signal == "LONG" else ("🔴" if result.final_signal == "SHORT" else "🟡")
    lines.append(f"{final_emoji} 최종 종합: {result.final_signal}")
    lines.append(f"신뢰도: {result.confidence}%")
    lines.append(f"종합 점수: {result.final_score:+.2f}")

    if result.recommended_expiry is not None:
        lines.append(f"🎯 추천 만기시간: {format_expiry_label(result.recommended_expiry)}")
    else:
        lines.append("🎯 추천 만기시간: 관망")

    lines.append("")

    if result.reason_lines:
        lines.append("🧩 주요 근거")
        lines.extend(result.reason_lines)
        lines.append("")

    lines.append("⚠ 참고")
    lines.append("- 이 봇은 자동 주문이 아니라 자동 신호 발송입니다.")
    lines.append("- OTC는 공개 OTC 캔들 대신 대응 원자산 프록시로 분석합니다.")
    lines.append("- 실전 투입 전 반드시 데모/소액으로 검증하세요.")
    return "\n".join(lines)


def format_auto_alert(strategy: str, market: str, asset_name: str, result: SignalResult) -> str:
    emoji = "🟢" if result.final_signal == "LONG" else "🔴"
    lines = [
        "🚨 자동신호 알림",
        "",
        f"전략: {STRATEGIES.get(strategy, strategy)}",
        f"시장: {MARKETS.get(market, market)}",
        f"종목: {asset_name}",
        f"현재가: {result.price:.6f}",
        f"{emoji} 최종 종합: {result.final_signal}",
        f"신뢰도: {result.confidence}%",
    ]

    if result.recommended_expiry is not None:
        lines.append(f"🎯 추천 만기시간: {format_expiry_label(result.recommended_expiry)}")
    else:
        lines.append("🎯 추천 만기시간: 관망")

    lines.extend(["", "만기별:"])

    for e in EXPIRIES:
        sig, sc = result.expiry_signals[e]
        lines.append(f"- {format_expiry_label(e)}: {sig} ({sc:+.2f})")

    return "\n".join(lines)


def format_auto_watchlist(chat_data: dict) -> str:
    auto_assets = chat_data.get("auto_assets", [])
    strategy = STRATEGIES.get(chat_data.get("strategy"), "미선택")
    market = MARKETS.get(chat_data.get("market"), "미선택")
    interval = chat_data.get("auto_interval", 60)
    running = "실행중" if chat_data.get("auto_running", False) else "정지"

    lines = [
        "🤖 자동신호 상태",
        "",
        f"전략: {strategy}",
        f"시장: {market}",
        f"주기: {interval}초",
        f"상태: {running}",
        "",
        "자동신호 종목:",
    ]

    if not auto_assets:
        lines.append("- 없음")
    else:
        for i, name in enumerate(auto_assets, 1):
            lines.append(f"{i}. {name}")

    return "\n".join(lines)


# =========================================================
# 상태 관리
# =========================================================
def reset_user_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["strategy"] = None
    context.user_data["market"] = None
    context.user_data["asset_name"] = None
    context.user_data["asset_code"] = None
    context.user_data["select_mode"] = "manual"

    context.chat_data["strategy"] = None
    context.chat_data["market"] = None
    context.chat_data["auto_assets"] = []
    context.chat_data["auto_interval"] = 60
    context.chat_data["auto_running"] = False
    context.chat_data["last_auto_signal"] = {}


def ensure_defaults(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("strategy", None)
    context.user_data.setdefault("market", None)
    context.user_data.setdefault("asset_name", None)
    context.user_data.setdefault("asset_code", None)
    context.user_data.setdefault("select_mode", "manual")

    context.chat_data.setdefault("strategy", None)
    context.chat_data.setdefault("market", None)
    context.chat_data.setdefault("auto_assets", [])
    context.chat_data.setdefault("auto_interval", 60)
    context.chat_data.setdefault("auto_running", False)
    context.chat_data.setdefault("last_auto_signal", {})


# =========================================================
# Job 관리
# =========================================================
def job_name_for_chat(chat_id: int) -> str:
    return f"auto_signal_{chat_id}"


def stop_auto_job(application: Application, chat_id: int) -> bool:
    removed = False
    for job in application.job_queue.get_jobs_by_name(job_name_for_chat(chat_id)):
        job.schedule_removal()
        removed = True
    return removed


async def auto_signal_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    data = job.data
    chat_id = data["chat_id"]
    strategy = data["strategy"]
    market = data["market"]
    asset_names = data["asset_names"]

    if not asset_names:
        return

    for asset_name in asset_names:
        try:
            asset_code = get_asset_code(market, asset_name)
            if not asset_code:
                continue

            df_raw, market_note = fetch_market_data(market, asset_code)
            df = add_indicators(df_raw)
            result = analyze_symbol(df, strategy, market_note)

            if result.final_signal not in ("LONG", "SHORT"):
                continue

            last_map = data.setdefault("last_signals", {})
            prev = last_map.get(asset_name)

            current_state = {
                "signal": result.final_signal,
                "score": round(result.final_score, 4),
                "recommended_expiry": result.recommended_expiry,
            }

            if prev == current_state:
                continue

            last_map[asset_name] = current_state

            msg = format_auto_alert(strategy, market, asset_name, result)
            await context.bot.send_message(chat_id=chat_id, text=msg)

        except Exception as e:
            err = f"자동신호 오류\n종목: {asset_name}\n오류: {type(e).__name__}: {e}"
            try:
                await context.bot.send_message(chat_id=chat_id, text=err)
            except Exception:
                pass


# =========================================================
# 키보드 UI
# =========================================================
def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("전략 선택", callback_data="menu_strategy")],
        [InlineKeyboardButton("도움말", callback_data="menu_help")],
    ])


def strategy_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1번 전략", callback_data="strategy_s1")],
        [InlineKeyboardButton("2번 전략", callback_data="strategy_s2")],
        [InlineKeyboardButton("3번 전략", callback_data="strategy_s3")],
        [InlineKeyboardButton("처음으로", callback_data="go_home")],
    ])


def market_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("실제화폐", callback_data="market_fx")],
        [InlineKeyboardButton("OTC", callback_data="market_otc")],
        [InlineKeyboardButton("암호화폐", callback_data="market_crypto")],
        [InlineKeyboardButton("처음으로", callback_data="go_home")],
    ])


def asset_keyboard(market: str, page: int = 0, mode: str = "manual") -> InlineKeyboardMarkup:
    items = get_assets_by_market(market)
    start = page * ASSETS_PER_PAGE
    end = start + ASSETS_PER_PAGE
    chunk = items[start:end]

    rows = []
    for name, _code in chunk:
        rows.append([InlineKeyboardButton(name, callback_data=f"asset|{mode}|{market}|{page}|{name}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ 이전", callback_data=f"asset_page|{mode}|{market}|{page-1}"))
    if end < len(items):
        nav.append(InlineKeyboardButton("다음 ➡", callback_data=f"asset_page|{mode}|{market}|{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("시장 다시 선택", callback_data="menu_market")])
    rows.append([InlineKeyboardButton("처음으로", callback_data="go_home")])
    return InlineKeyboardMarkup(rows)


def selected_asset_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("수동 시그널 요청", callback_data="request_signal")],
        [InlineKeyboardButton("자동신호 종목 추가", callback_data="auto_add_mode")],
        [InlineKeyboardButton("자동신호 목록 보기", callback_data="auto_list")],
        [InlineKeyboardButton("자동신호 시작", callback_data="auto_start_preset")],
        [InlineKeyboardButton("자동신호 정지", callback_data="auto_stop")],
        [InlineKeyboardButton("다른 종목 선택", callback_data="menu_asset")],
        [InlineKeyboardButton("처음으로", callback_data="go_home")],
    ])


def auto_interval_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"{sec}초", callback_data=f"auto_interval|{sec}")] for sec in AUTO_INTERVALS]
    rows.append([InlineKeyboardButton("뒤로", callback_data="auto_list")])
    rows.append([InlineKeyboardButton("처음으로", callback_data="go_home")])
    return InlineKeyboardMarkup(rows)


# =========================================================
# 명령어 핸들러
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_defaults(context)
    text = (
        "안녕하세요.\n\n"
        "사용 순서:\n"
        "1) 전략 선택\n"
        "2) 시장 선택\n"
        "3) 종목 선택\n"
        "4) 수동 시그널 요청 또는 자동신호 종목 추가\n\n"
        "자동신호는 LONG/SHORT일 때만 발송됩니다.\n"
        "검색은 /find EURUSD 또는 /find BTC 처럼 사용하세요."
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "사용법\n\n"
        "/start - 시작\n"
        "/find 검색어 - 종목 검색\n"
        "/state - 현재 선택 상태 확인\n"
        "/reset - 선택 초기화\n\n"
        "만기시간 분석:\n"
        "1분, 2분, 3분, 5분, 7분, 10분, 15분, 30분, 1시간, 4시간, 1일\n\n"
        "자동신호 흐름\n"
        "전략 선택 → 시장 선택 → 종목 선택 → 자동신호 종목 추가 → 자동신호 시작"
    )
    await update.message.reply_text(text, reply_markup=main_menu_keyboard())


async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_defaults(context)
    if update.effective_chat:
        stop_auto_job(context.application, update.effective_chat.id)
    reset_user_state(context)
    await update.message.reply_text("초기화했습니다.", reply_markup=main_menu_keyboard())


async def state_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_defaults(context)
    s = context.user_data
    text = (
        f"현재 상태\n\n"
        f"전략: {STRATEGIES.get(s.get('strategy'), '미선택')}\n"
        f"시장: {MARKETS.get(s.get('market'), '미선택')}\n"
        f"수동 종목: {s.get('asset_name') or '미선택'}\n\n"
        f"{format_auto_watchlist(context.chat_data)}"
    )
    await update.message.reply_text(text, reply_markup=selected_asset_menu())


async def find_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_defaults(context)

    if not context.args:
        await update.message.reply_text("예: /find EURUSD 또는 /find BTC")
        return

    keyword = " ".join(context.args).strip().upper().replace(" ", "")
    all_items = []

    for market_key in ["fx", "otc", "crypto"]:
        for name, code in get_assets_by_market(market_key):
            key1 = name.upper().replace("/", "").replace(" ", "")
            key2 = code.upper().replace("/", "").replace(" ", "")
            if keyword in key1 or keyword in key2:
                all_items.append((market_key, name, code))

    if not all_items:
        await update.message.reply_text("검색 결과가 없습니다.")
        return

    rows = []
    for market_key, name, _code in all_items[:20]:
        rows.append([InlineKeyboardButton(f"[{MARKETS[market_key]}] {name}", callback_data=f"findpick|{market_key}|{name}")])

    rows.append([InlineKeyboardButton("처음으로", callback_data="go_home")])
    await update.message.reply_text("검색 결과", reply_markup=InlineKeyboardMarkup(rows))


async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "버튼으로 선택하거나 /find 종목명 으로 검색하세요.\n예: /find EURUSD",
        reply_markup=main_menu_keyboard()
    )


# =========================================================
# 콜백 핸들러
# =========================================================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_defaults(context)
    query = update.callback_query
    await query.answer()
    data = query.data

    try:
        if data == "menu_strategy":
            await query.edit_message_text("전략을 선택하세요.", reply_markup=strategy_keyboard())
            return

        if data == "menu_market":
            if not context.user_data.get("strategy"):
                await query.edit_message_text("먼저 전략을 선택하세요.", reply_markup=strategy_keyboard())
                return
            await query.edit_message_text("시장을 선택하세요.", reply_markup=market_keyboard())
            return

        if data == "menu_asset":
            market = context.user_data.get("market")
            if not market:
                await query.edit_message_text("먼저 시장을 선택하세요.", reply_markup=market_keyboard())
                return
            context.user_data["select_mode"] = "manual"
            await query.edit_message_text(
                f"{MARKETS[market]} 종목을 선택하세요. (수동용 / 알파벳 순)",
                reply_markup=asset_keyboard(market, 0, "manual"),
            )
            return

        if data == "menu_help":
            await query.edit_message_text(
                "사용 순서:\n1) 전략 선택\n2) 시장 선택\n3) 종목 선택\n4) 수동 시그널 요청 또는 자동신호 추가\n\n"
                "검색: /find EURUSD 또는 /find BTC\n"
                "초기화: /reset",
                reply_markup=main_menu_keyboard(),
            )
            return

        if data == "go_home":
            await query.edit_message_text("메인 메뉴", reply_markup=main_menu_keyboard())
            return

        if data.startswith("strategy_"):
            strategy = data.split("_", 1)[1]
            context.user_data["strategy"] = strategy
            context.chat_data["strategy"] = strategy
            context.user_data["market"] = None
            context.user_data["asset_name"] = None
            context.user_data["asset_code"] = None
            context.chat_data["market"] = None
            context.chat_data["auto_assets"] = []
            context.chat_data["last_auto_signal"] = {}
            context.chat_data["auto_running"] = False

            if update.effective_chat:
                stop_auto_job(context.application, update.effective_chat.id)

            await query.edit_message_text(
                f"선택된 전략:\n{STRATEGIES[strategy]}\n\n이제 시장을 선택하세요.",
                reply_markup=market_keyboard()
            )
            return

        if data.startswith("market_"):
            market = data.split("_", 1)[1]
            context.user_data["market"] = market
            context.chat_data["market"] = market
            context.user_data["asset_name"] = None
            context.user_data["asset_code"] = None
            context.chat_data["auto_assets"] = []
            context.chat_data["last_auto_signal"] = {}

            await query.edit_message_text(
                f"선택된 시장: {MARKETS[market]}\n\n수동용 종목을 먼저 하나 선택하세요.",
                reply_markup=asset_keyboard(market, 0, "manual")
            )
            return

        if data.startswith("asset_page|"):
            _, mode, market, page_str = data.split("|", 3)
            page = int(page_str)
            title = "자동신호용 종목 선택" if mode == "auto" else "수동용 종목 선택"
            await query.edit_message_text(
                f"{MARKETS[market]} {title} (알파벳 순)",
                reply_markup=asset_keyboard(market, page, mode)
            )
            return

        if data.startswith("asset|"):
            _, mode, market, _page, asset_name = data.split("|", 4)
            asset_code = get_asset_code(market, asset_name)

            if not asset_code:
                await query.edit_message_text("종목 정보를 찾을 수 없습니다.", reply_markup=main_menu_keyboard())
                return

            if mode == "manual":
                context.user_data["market"] = market
                context.user_data["asset_name"] = asset_name
                context.user_data["asset_code"] = asset_code
                context.user_data["select_mode"] = "manual"

                await query.edit_message_text(
                    f"선택 완료\n\n"
                    f"전략: {STRATEGIES[context.user_data['strategy']]}\n"
                    f"시장: {MARKETS[market]}\n"
                    f"수동 종목: {asset_name}\n\n"
                    f"아래 메뉴를 선택하세요.",
                    reply_markup=selected_asset_menu()
                )
                return

            if mode == "auto":
                auto_assets = context.chat_data.get("auto_assets", [])
                if asset_name not in auto_assets:
                    auto_assets.append(asset_name)
                    auto_assets.sort(key=lambda x: x.upper())
                    context.chat_data["auto_assets"] = auto_assets

                await query.edit_message_text(
                    f"자동신호 종목 추가 완료\n\n"
                    f"추가된 종목: {asset_name}\n\n"
                    f"{format_auto_watchlist(context.chat_data)}",
                    reply_markup=selected_asset_menu()
                )
                return

        if data.startswith("findpick|"):
            _, market, asset_name = data.split("|", 2)
            asset_code = get_asset_code(market, asset_name)

            if not asset_code:
                await query.edit_message_text("검색 선택 실패", reply_markup=main_menu_keyboard())
                return

            if not context.user_data.get("strategy"):
                context.user_data["market"] = market
                context.user_data["asset_name"] = asset_name
                context.user_data["asset_code"] = asset_code
                await query.edit_message_text(
                    f"종목은 골랐지만 전략이 아직 없습니다.\n"
                    f"종목: {asset_name}\n\n"
                    f"먼저 전략을 선택하세요.",
                    reply_markup=strategy_keyboard()
                )
                return

            context.user_data["market"] = market
            context.chat_data["market"] = market
            context.user_data["asset_name"] = asset_name
            context.user_data["asset_code"] = asset_code
            await query.edit_message_text(
                f"선택 완료\n\n"
                f"전략: {STRATEGIES[context.user_data['strategy']]}\n"
                f"시장: {MARKETS[market]}\n"
                f"수동 종목: {asset_name}\n\n"
                f"아래 메뉴를 선택하세요.",
                reply_markup=selected_asset_menu()
            )
            return

        if data == "request_signal":
            strategy = context.user_data.get("strategy")
            market = context.user_data.get("market")
            asset_name = context.user_data.get("asset_name")
            asset_code = context.user_data.get("asset_code")

            if not strategy:
                await query.edit_message_text("먼저 전략을 선택하세요.", reply_markup=strategy_keyboard())
                return
            if not market:
                await query.edit_message_text("먼저 시장을 선택하세요.", reply_markup=market_keyboard())
                return
            if not asset_name or not asset_code:
                await query.edit_message_text("먼저 종목을 선택하세요.", reply_markup=asset_keyboard(market, 0, "manual"))
                return

            await query.edit_message_text(
                f"분석 중...\n\n"
                f"전략: {STRATEGIES[strategy]}\n"
                f"시장: {MARKETS[market]}\n"
                f"종목: {asset_name}"
            )

            df_raw, market_note = fetch_market_data(market, asset_code)
            df = add_indicators(df_raw)
            result = analyze_symbol(df, strategy, market_note)
            text = format_signal_message(context.user_data, result)

            await query.edit_message_text(text, reply_markup=selected_asset_menu())
            return

        if data == "auto_add_mode":
            strategy = context.user_data.get("strategy")
            market = context.user_data.get("market")
            if not strategy:
                await query.edit_message_text("먼저 전략을 선택하세요.", reply_markup=strategy_keyboard())
                return
            if not market:
                await query.edit_message_text("먼저 시장을 선택하세요.", reply_markup=market_keyboard())
                return

            context.user_data["select_mode"] = "auto"
            await query.edit_message_text(
                f"{MARKETS[market]} 자동신호용 종목을 선택하세요. 여러 개 추가 가능합니다.",
                reply_markup=asset_keyboard(market, 0, "auto")
            )
            return

        if data == "auto_list":
            await query.edit_message_text(
                format_auto_watchlist(context.chat_data),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("자동신호 종목 추가", callback_data="auto_add_mode")],
                    [InlineKeyboardButton("자동신호 시작", callback_data="auto_start_preset")],
                    [InlineKeyboardButton("자동신호 정지", callback_data="auto_stop")],
                    [InlineKeyboardButton("수동 메뉴로", callback_data="back_selected_menu")],
                ])
            )
            return

        if data == "back_selected_menu":
            await query.edit_message_text("메뉴를 선택하세요.", reply_markup=selected_asset_menu())
            return

        if data == "auto_start_preset":
            strategy = context.user_data.get("strategy")
            market = context.user_data.get("market")
            auto_assets = context.chat_data.get("auto_assets", [])

            if not strategy:
                await query.edit_message_text("먼저 전략을 선택하세요.", reply_markup=strategy_keyboard())
                return
            if not market:
                await query.edit_message_text("먼저 시장을 선택하세요.", reply_markup=market_keyboard())
                return
            if not auto_assets:
                await query.edit_message_text(
                    "자동신호 종목이 없습니다.\n먼저 '자동신호 종목 추가'로 종목을 넣어주세요.",
                    reply_markup=selected_asset_menu()
                )
                return

            await query.edit_message_text(
                "자동신호 주기를 선택하세요.",
                reply_markup=auto_interval_keyboard()
            )
            return

        if data.startswith("auto_interval|"):
            interval = int(data.split("|", 1)[1])
            chat = update.effective_chat
            if not chat:
                await query.edit_message_text("채팅 정보를 찾을 수 없습니다.", reply_markup=main_menu_keyboard())
                return

            strategy = context.user_data.get("strategy")
            market = context.user_data.get("market")
            auto_assets = list(context.chat_data.get("auto_assets", []))

            stop_auto_job(context.application, chat.id)

            context.chat_data["auto_interval"] = interval
            context.chat_data["auto_running"] = True
            context.chat_data["last_auto_signal"] = {}

            context.application.job_queue.run_repeating(
                auto_signal_job,
                interval=interval,
                first=1,
                name=job_name_for_chat(chat.id),
                data={
                    "chat_id": chat.id,
                    "strategy": strategy,
                    "market": market,
                    "asset_names": auto_assets,
                    "last_signals": {},
                },
            )

            await query.edit_message_text(
                f"자동신호 시작 완료\n\n"
                f"전략: {STRATEGIES[strategy]}\n"
                f"시장: {MARKETS[market]}\n"
                f"주기: {interval}초\n"
                f"종목 수: {len(auto_assets)}개\n\n"
                f"LONG/SHORT 신호와 추천 만기시간을 자동으로 보냅니다.",
                reply_markup=selected_asset_menu()
            )
            return

        if data == "auto_stop":
            chat = update.effective_chat
            if not chat:
                await query.edit_message_text("채팅 정보를 찾을 수 없습니다.", reply_markup=main_menu_keyboard())
                return

            removed = stop_auto_job(context.application, chat.id)
            context.chat_data["auto_running"] = False

            msg = "자동신호를 정지했습니다." if removed else "실행 중인 자동신호가 없습니다."
            await query.edit_message_text(msg, reply_markup=selected_asset_menu())
            return

        await query.edit_message_text("알 수 없는 동작입니다.", reply_markup=main_menu_keyboard())

    except Exception as e:
        logger.error("callback error: %s\n%s", e, traceback.format_exc())
        try:
            await query.edit_message_text(
                f"오류가 발생했습니다.\n\n{type(e).__name__}: {e}\n\n다시 시도해 주세요.",
                reply_markup=main_menu_keyboard()
            )
        except Exception:
            pass


# =========================================================
# 에러 핸들러
# =========================================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception: %s", context.error)
    logger.error(traceback.format_exc())


# =========================================================
# 실행
# =========================================================
def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("reset", reset_cmd))
    app.add_handler(CommandHandler("state", state_cmd))
    app.add_handler(CommandHandler("find", find_cmd))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))

    app.add_error_handler(error_handler)

    print("텔레그램 시그널 봇 실행 중...")
    app.run_polling()


if __name__ == "__main__":
    main()
