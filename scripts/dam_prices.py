# -*- coding: utf-8 -*-
"""
UA OREE DAM — end-to-end downloader + parser (PHASE 1 + PHASE 2)

This script builds an hourly time series of Ukraine OREE day-ahead market (DAM) prices by:
(1) downloading the daily OREE XLS files into ./temp (no parsing in PHASE 1),
(2) converting XLS -> XLSX in bulk (fast path via one Excel COM instance; fallback to pandas),
(3) parsing each daily table into long-form rows (date + hour_seq + price + optional volumes),
(4) creating DST-safe timestamps in Europe/Kyiv and converting to UTC + fixed CET (UTC+1),
(5) fetching National Bank of Ukraine (NBU) EUR FX rates (UAH per 1 EUR) per market date,
    caching them to disk, and converting UAH/MWh -> EUR/MWh,
(6) exporting two Excel files: a “clean” AL-style file and a “full” file with more columns.

----------------------------------------
PHASE 1 — Download daily XLS into ./temp
----------------------------------------
- Iterates from START_DATE = Jan 1 of START_YEAR up to END_DATE = today (inclusive).
- For each day d builds the OREE URL:
    https://www.oree.com.ua/index.php/PXS/downloadxlsx/DD.MM.YYYY/DAM/{IDX}
- Uses a requests.Session with browser-like headers (User-Agent, Referer, etc.).
- Skips days already downloaded:
    ./temp/DAM_DD.MM.YYYY_idx{IDX}.xls
- Saves server bytes as an .xls file (server filename if provided via Content-Disposition,
  otherwise deterministic name), always including the index suffix:
    DAM_DD.MM.YYYY_idx{IDX}.xls
- Flags suspicious “stub” downloads (e.g., too small or not OLE header) by:
    - appending _STUB to the filename, and
    - incrementing stub counters / logging.
  (Heuristic: length < MIN_BYTES_GOOD or first 8 bytes != OLE_HEADER)
- Throttles requests with SLEEP_S between days; counts ok / stub / failed.

----------------------------------------
PHASE 2 — Parse XLS -> hourly long-form + EUR/MWh
----------------------------------------
Inputs:
- Reads the PHASE 1 outputs from:
    ./temp/DAM_*_idx*.xls  (fallback: ./temp/DAM_*.xls)
- Creates/uses conversion folder:
    ./temp/_converted_xlsx/

Step A) Convert all XLS -> XLSX
- Fast path (Windows): uses pywin32 + a single hidden Excel COM instance to open each .xls
  and SaveAs FileFormat=51 into ./temp/_converted_xlsx/.
- Skips conversion if the .xlsx exists and is newer than the .xls.
- Fallback path: for any file that cannot be converted via COM (or if pywin32 is missing),
  reads the first sheet with pandas/xlrd and writes .xlsx with openpyxl.

Step B) Parse each converted daily XLSX
- Extracts the market date from filename pattern: DAM_DD.MM.YYYY...
- Reads the sheet (no header), detects the real header row by searching for “Година/Hour”
  plus “Ціна/Price” and optionally volume columns (“Обсяг ...”).
- Selects columns by keyword matching (hour / price / sell volume / buy volume).
- Extracts hour_seq from strings like "01:00", "24:00", "25:00" (valid range 1..25),
  supporting DST fall-back days with 25 hours.
- Parses Ukrainian-formatted numbers like "5 600,00" -> 5600.0.
- Produces per-row fields:
    date (market day), hour_seq, price_uah_mwh, vol_sell_mwh (optional), vol_buy_mwh (optional)
- Drops rows without valid hour_seq or price.

Step C) Build DST-safe timestamps + timezone conversions
- For each market_date group:
    - localizes midnight at Europe/Kyiv
    - generates an hourly range with periods = max(hour_seq) (23/24/25)
      to correctly represent DST transitions (including repeated hour on fall-back).
    - maps hour_seq 1..n to the corresponding timestamp in that range.
- Creates:
    ts_kyiv (tz-aware), ts_utc (converted), ts_cet_fixed (converted to constant UTC+1),
    ts_cet_fixed_naive (tz removed for sorting/export)

Step D) Fetch NBU EUR FX rates + convert currency
- Uses NBUStatService exchange endpoint to fetch EUR rate for each market_date:
    UAH per 1 EUR
- Caches fetched rates to:
    ./_nbu_eur_cache.csv
  and only requests missing dates on subsequent runs.
- Maps uah_per_eur onto every hourly row; computes:
    price_eur_mwh = price_uah_mwh / uah_per_eur

Step E) Export results (two Excel files)
- Sorts by ts_cet_fixed_naive.
- Writes:
  1) FULL file (more columns):
      UA_OREE_DAM_hourly_prices_{YYYY or YYYY_YYYY}_more_info.xlsx
     with timestamp columns, market_date, hour_seq, UAH price, FX, EUR price, volumes.
  2) CLEAN file (AL-style):
      UA_OREE_DAM_hourly_prices_{YYYY or YYYY_YYYY}.xlsx
     columns:
      date, hour, delivery_start_CET, price_eur_mwh, market, extracted_at
- Before writing Excel, converts tz-aware timestamps to strings to avoid Excel TZ issues.

Notes / Requirements:
- PHASE 2 fast conversion requires Windows + MS Excel + pywin32.
- pandas fallback conversion requires xlrd (for .xls) + openpyxl (for .xlsx writing).
- FIXED_CET_TZ is constant UTC+1 ("Etc/GMT-1"). If you want DST-aware Berlin time,
  use "Europe/Berlin" instead.
"""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path
import shutil
import subprocess

import requests
from tqdm import tqdm


BASE_DIR = Path(__file__).resolve().parent.parent

PERIOD_DIR: Path | None = None
TEMP_DIR: Path | None = None
CONVERTED_DIR: Path | None = None
FX_CACHE_CSV: Path | None = None


# -----------------------
# SETTINGS - CONFIGURABLE
# -----------------------


# *** CONFIGURE YOUR DATE RANGE HERE ***
START_YEAR = 2026  # Year to start downloading from (e.g., 2025)
END_DATE = date.today()  # Download up to today

# Calculate actual start date (January 1st of start year)
START_DATE = date(START_YEAR, 1, 1)

IDX = 2  # use the working index you tested
BASE_URL = "https://www.oree.com.ua/index.php/PXS/downloadxlsx"

def set_period_dirs(period: str) -> None:
    global PERIOD_DIR, TEMP_DIR, CONVERTED_DIR, FX_CACHE_CSV
    PERIOD_DIR = BASE_DIR / "data" / period
    TEMP_DIR = PERIOD_DIR / "dam_prices" / "temp"
    CONVERTED_DIR = TEMP_DIR / "_converted_xlsx"
    FX_CACHE_CSV = PERIOD_DIR / "dam_prices" / "_nbu_eur_cache.csv"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    CONVERTED_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 60
SLEEP_S = 0.12

# Heuristics / signatures
OLE_HEADER = b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"
MIN_BYTES_GOOD = 7200  # your good example was 7680; stubs were ~6656


# -----------------------
# HELPERS
# -----------------------
def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def is_probably_real_xls(content: bytes) -> bool:
    # Most OREE files are classic BIFF .xls (OLE container)
    if not content:
        return False
    if len(content) < MIN_BYTES_GOOD:
        return False
    return content[:8] == OLE_HEADER


def safe_filename_from_cd(content_disposition: str) -> str | None:
    """
    Extract filename=... from Content-Disposition if present.
    Example: attachment;filename="DAM_15.01.2026.xls"
    """
    if not content_disposition:
        return None
    m = re.search(r'filename\s*=\s*"?([^";]+)"?', content_disposition, flags=re.I)
    if not m:
        return None
    return m.group(1).strip()


# -----------------------
# MAIN
# -----------------------
def run_phase1():
    print("### UA OREE DAM – downloader only (loop + save to temp) ###")
    if TEMP_DIR is None:
        raise RuntimeError("TEMP_DIR is not set. Call set_period_dirs() first.")
    print(f"Workdir: {BASE_DIR}")
    print(f"Date range: {START_DATE} -> {END_DATE}")
    print(f"Index: {IDX}")
    print(f"Temp folder: {TEMP_DIR.resolve()}")

    s = requests.Session()

    # Same idea as your single-day "works" test:
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
        ),
        "Accept": "*/*",
        "Referer": "https://www.oree.com.ua/",
        "Accept-Language": "en-US,en;q=0.9,uk;q=0.8",
        "Connection": "keep-alive",
    })

    ok = 0
    stub = 0
    failed = 0

    for d in tqdm(list(daterange(START_DATE, END_DATE)), desc="Downloading days"):
        d_str = d.strftime("%d.%m.%Y")
        url = f"{BASE_URL}/{d_str}/DAM/{IDX}"
        
        # Check if file already exists
        expected_filename = f"DAM_{d_str}_idx{IDX}.xls"
        expected_path = TEMP_DIR / expected_filename
        
        if expected_path.exists():
            # File already downloaded, skip
            ok += 1
            continue

        try:
            r = s.get(url, timeout=TIMEOUT, allow_redirects=True)
            status = r.status_code

            if status != 200:
                failed += 1
                tqdm.write(f"❌ {d_str} HTTP {status} -> {url}")
                time.sleep(SLEEP_S)
                continue

            content = r.content or b""
            cd = r.headers.get("Content-Disposition", "")
            fname_from_server = safe_filename_from_cd(cd)

            # Use server filename if available; otherwise a deterministic one
            base_name = fname_from_server or f"DAM_{d_str}.xls"

            # We also add idx in name to keep it explicit
            # Example: DAM_15.01.2026_idx2.xls
            stem = base_name.replace(".xls", "")
            out_name = f"{stem}_idx{IDX}.xls"

            # Tag suspicious downloads
            if not is_probably_real_xls(content):
                out_name = out_name.replace(".xls", "_STUB.xls")
                stub += 1
            else:
                ok += 1

            out_path = TEMP_DIR / out_name
            out_path.write_bytes(content)

        except Exception as e:
            failed += 1
            tqdm.write(f"❌ {d_str} ERROR {type(e).__name__}: {e}")

        time.sleep(SLEEP_S)

    print("\nDone.")
    print(f"✅ Good-looking XLS saved: {ok}")
    print(f"⚠️  Stub/suspicious saved: {stub}")
    print(f"❌ Failed: {failed}")
    print(f"Files are in: {TEMP_DIR.resolve()}")


# =============================
# PHASE 2 — Parse daily XLS -> long-form fixed CET + EUR/MWh (FAST)
# =============================

import os
import re
import time
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# ---------- CONFIG ----------

# Dynamic filename based on year range
# Will be set in run_phase2() after parsing dates

KYIV_TZ = "Europe/Kyiv"
FIXED_CET_TZ = "Etc/GMT-1"   # constant UTC+1 (fixed CET). If you want Berlin DST: "Europe/Berlin"

# NBU EUR FX (UAH per 1 EUR)
NBU_EUR_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange"
NBU_TIMEOUT = 20
NBU_RETRIES = 3
NBU_SLEEP_BETWEEN = 0.8


# ---------- HELPERS ----------
def parse_ua_number(x):
    """Parse '5 600,00' -> 5600.0 ; returns float or np.nan."""
    if x is None:
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        try:
            return float(x)
        except Exception:
            return np.nan
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return np.nan
    s = s.replace("\u00a0", " ").replace(" ", "")  # remove thousands spaces
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in {"", "-", ".", "-."}:
        return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan


def extract_hour_seq(v):
    """
    Extract sequential hour index from strings like '01:00', '24:00', '25:00'.
    Returns int 1..25 or None.
    """
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    m = re.search(r"(\d{1,2})\s*:\s*\d{2}", s)
    if not m:
        return None
    h = int(m.group(1))
    return h if 1 <= h <= 25 else None


def find_header_row(raw_df: pd.DataFrame) -> int:
    """Find row containing header (Година + (Ціна/Обсяг))."""
    for i in range(min(len(raw_df), 80)):
        row = raw_df.iloc[i].astype(str).str.lower()
        joined = " | ".join(row.tolist())
        if ("година" in joined or "hour" in joined or "час" in joined) and (
            "ціна" in joined or "price" in joined or "обсяг" in joined or "volume" in joined
        ):
            return i
    for i in range(min(len(raw_df), 80)):
        joined = " | ".join(raw_df.iloc[i].astype(str).str.lower().tolist())
        if "година" in joined or "hour" in joined or "час" in joined:
            return i
    return -1


def pick_col(columns, keywords):
    cols_lower = {c: str(c).strip().lower() for c in columns}
    for c, cl in cols_lower.items():
        if any(k in cl for k in keywords):
            return c
    return None


# ---------- FAST XLS -> XLSX conversion (one Excel instance) ----------
def convert_all_xls_to_xlsx_fast(xls_files):
    """
    Convert all .xls to .xlsx using a single Excel COM instance (much faster).
    Falls back to pandas if COM fails.
    Requires: pywin32 + MS Excel installed (Windows).
    """
    try:
        import win32com.client  # noqa
        use_excel_com = True
    except ImportError:
        print("⚠️ pywin32 not available, will use pandas for conversion (slower)")
        use_excel_com = False

    converted = []
    failed_com = []
    
    if use_excel_com:
        excel = None
        try:
            excel = win32com.client.DispatchEx("Excel.Application")
            excel.Visible = False
            excel.DisplayAlerts = False

            for xls_path in tqdm(xls_files, desc="Converting XLS->XLSX (Excel COM)"):
                xls_path = Path(xls_path)
                xlsx_path = CONVERTED_DIR / (xls_path.stem + ".xlsx")

                # skip if up-to-date
                if xlsx_path.exists():
                    try:
                        if xlsx_path.stat().st_mtime >= xls_path.stat().st_mtime:
                            converted.append(xlsx_path)
                            continue
                    except Exception:
                        pass

                wb = None
                try:
                    wb = excel.Workbooks.Open(str(xls_path.absolute()), ReadOnly=True)
                    # 51 = xlOpenXMLWorkbook (.xlsx)
                    wb.SaveAs(str(xlsx_path.absolute()), FileFormat=51)
                    converted.append(xlsx_path)
                except Exception as e:
                    tqdm.write(f"⚠️ COM failed for {xls_path.name}: {e}")
                    failed_com.append(xls_path)
                finally:
                    try:
                        if wb is not None:
                            wb.Close(SaveChanges=False)
                    except Exception:
                        pass

        finally:
            try:
                if excel is not None:
                    excel.Quit()
            except Exception:
                pass
    else:
        failed_com = xls_files  # All files need pandas conversion

    # Fallback: Use LibreOffice (soffice) on macOS/Linux if available
    if failed_com:
        soffice = shutil.which("soffice")
        if soffice:
            print(f"\n📊 Converting {len(failed_com)} files with LibreOffice (soffice)...")
            for xls_path in tqdm(failed_com, desc="Converting XLS->XLSX (soffice)"):
                xls_path = Path(xls_path)
                xlsx_path = CONVERTED_DIR / (xls_path.stem + ".xlsx")
                if xlsx_path.exists():
                    try:
                        if xlsx_path.stat().st_mtime >= xls_path.stat().st_mtime:
                            converted.append(xlsx_path)
                            continue
                    except Exception:
                        pass
                try:
                    cmd = [
                        soffice,
                        "--headless",
                        "--convert-to",
                        "xlsx",
                        "--outdir",
                        str(CONVERTED_DIR),
                        str(xls_path),
                    ]
                    subprocess.run(cmd, check=True, capture_output=True, text=True)
                    if xlsx_path.exists():
                        converted.append(xlsx_path)
                    else:
                        tqdm.write(f"❌ soffice did not produce {xlsx_path.name}")
                except Exception as e:
                    tqdm.write(f"❌ soffice failed for {xls_path.name}: {e}")
        else:
            print("\n⚠️ LibreOffice (soffice) not found; falling back to pandas (xlrd) conversion.")

    # Final fallback: Use pandas for files that still failed conversion
    remaining = [p for p in failed_com if (CONVERTED_DIR / (Path(p).stem + ".xlsx")).exists() is False]
    if remaining:
        print(f"\n📊 Converting {len(remaining)} files with pandas (slower, needs xlrd)...")
        for xls_path in tqdm(remaining, desc="Converting XLS->XLSX (pandas)"):
            xls_path = Path(xls_path)
            xlsx_path = CONVERTED_DIR / (xls_path.stem + ".xlsx")
            if xlsx_path.exists():
                try:
                    if xlsx_path.stat().st_mtime >= xls_path.stat().st_mtime:
                        converted.append(xlsx_path)
                        continue
                except Exception:
                    pass
            try:
                df = pd.read_excel(xls_path, sheet_name=0, header=None, engine="xlrd")
                df.to_excel(xlsx_path, index=False, header=False, engine="openpyxl")
                converted.append(xlsx_path)
            except Exception as e:
                tqdm.write(f"❌ Failed to convert {xls_path.name}: {e}")

    return converted


# ---------- Parse one converted XLSX ----------
def parse_one_converted_xlsx(xlsx_path: Path) -> pd.DataFrame:
    """
    Parse table from converted .xlsx and return standardized daily rows.
    """
    # date from filename: DAM_DD.MM.YYYY_idx2 or DAM_DD.MM.YYYY
    m = re.search(r"DAM_(\d{2})\.(\d{2})\.(\d{4})", xlsx_path.name)
    if not m:
        raise ValueError(f"Cannot parse date from filename: {xlsx_path.name}")
    
    # Extract date parts
    day = int(m.group(1))
    month = int(m.group(2))
    year = int(m.group(3))
    
    # Create pandas Timestamp instead of Python datetime (better for pandas)
    d = pd.Timestamp(year=year, month=month, day=day)
    
    # DEBUG: Verify the timestamp is valid
    if pd.isna(d):
        raise ValueError(f"Created NaT timestamp from {year}-{month:02d}-{day:02d} in {xlsx_path.name}")

    raw = pd.read_excel(xlsx_path, header=None, dtype=object, engine="openpyxl")

    header_idx = find_header_row(raw)
    if header_idx < 0:
        raise ValueError(f"Header row not found in {xlsx_path.name}")

    header = raw.iloc[header_idx].astype(str).str.strip().tolist()
    body = raw.iloc[header_idx + 1 :].copy()
    body.columns = header

    col_hour = pick_col(body.columns, ["година", "hour", "час"])
    col_price = pick_col(body.columns, ["ціна", "price"])
    col_sell = pick_col(body.columns, ["обсяг продаж", "sale", "продаж"])
    col_buy  = pick_col(body.columns, ["обсяг куп", "buy", "купів"])

    if col_hour is None or col_price is None:
        raise ValueError(f"Required columns missing in {xlsx_path.name}. Columns={list(body.columns)}")

    # Create DataFrame with explicit date assignment
    num_rows = len(body)
    df = pd.DataFrame({
        'date': [d] * num_rows,  # Repeat the timestamp for all rows
        'hour_seq': body[col_hour].apply(extract_hour_seq)
    })
    
    # DEBUG: Check if date column is properly set
    if df['date'].isna().any():
        print(f"  ⚠️ WARNING: NaT detected in date column after assignment for {xlsx_path.name}")
        print(f"     Original timestamp d = {d}, type = {type(d)}")
        print(f"     df['date'] dtype = {df['date'].dtype}")
        print(f"     df['date'].head() = {df['date'].head().tolist()}")
    
    df = df.dropna(subset=["hour_seq"]).copy()
    df["hour_seq"] = df["hour_seq"].astype(int)
    df = df[(df["hour_seq"] >= 1) & (df["hour_seq"] <= 25)].copy()

    df["price_uah_mwh"] = body.loc[df.index, col_price].apply(parse_ua_number)

    if col_sell is not None:
        df["vol_sell_mwh"] = body.loc[df.index, col_sell].apply(parse_ua_number)
    if col_buy is not None:
        df["vol_buy_mwh"] = body.loc[df.index, col_buy].apply(parse_ua_number)

    df = df.dropna(subset=["price_uah_mwh"]).copy()
    
    # DEBUG: Print what we parsed
    print(f"  📅 {xlsx_path.name}: {d.date()} -> {len(df)} hours (seq {df['hour_seq'].min()}-{df['hour_seq'].max()}), date_valid={not df['date'].isna().any()}")
    
    return df


# ---------- NBU FX cache ----------
def load_fx_cache() -> dict:
    cache = {}
    if FX_CACHE_CSV.exists():
        try:
            cdf = pd.read_csv(FX_CACHE_CSV)
            if {"date", "uah_per_eur"}.issubset(cdf.columns):
                for _, r in cdf.iterrows():
                    try:
                        dd = datetime.strptime(str(r["date"]), "%Y-%m-%d").date()
                        cache[dd] = float(r["uah_per_eur"])
                    except Exception:
                        continue
        except Exception:
            pass
    return cache


def save_fx_cache(cache: dict):
    rows = [{"date": d.isoformat(), "uah_per_eur": v} for d, v in sorted(cache.items())]
    pd.DataFrame(rows).to_csv(FX_CACHE_CSV, index=False, encoding="utf-8")


def fetch_nbu_eur_rate(d: date) -> float:
    ymd = d.strftime("%Y%m%d")
    params = {"valcode": "EUR", "date": ymd, "json": ""}

    last_err = None
    for _ in range(NBU_RETRIES):
        try:
            r = requests.get(NBU_EUR_URL, params=params, timeout=NBU_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data or "rate" not in data[0]:
                raise ValueError(f"No rate in response for {d}: {data[:1]}")
            return float(data[0]["rate"])  # UAH per 1 EUR
        except Exception as e:
            last_err = e
            time.sleep(NBU_SLEEP_BETWEEN)
    raise RuntimeError(f"Failed to fetch NBU EUR rate for {d}: {last_err}")


def get_eur_rates(dates: list[date]) -> dict:
    cache = load_fx_cache()
    needed = sorted(set(dates))
    missing = [d for d in needed if d not in cache]

    if missing:
        for d in tqdm(missing, desc="Fetching NBU EUR rates"):
            cache[d] = fetch_nbu_eur_rate(d)
        save_fx_cache(cache)

    return {d: cache[d] for d in needed}


# ---------- MAIN PHASE 2 ----------
def run_phase2():
    if TEMP_DIR is None or CONVERTED_DIR is None or FX_CACHE_CSV is None:
        raise RuntimeError("Period directories are not set. Call set_period_dirs() first.")
    if not TEMP_DIR.exists():
        raise FileNotFoundError(f"temp folder not found: {TEMP_DIR}")

    # Find all XLS files (including those with _idx2 suffix)
    xls_files = sorted(TEMP_DIR.glob("DAM_*_idx*.xls"))
    if not xls_files:
        # Fallback: try without idx pattern
        xls_files = sorted(TEMP_DIR.glob("DAM_*.xls"))
    
    if not xls_files:
        raise FileNotFoundError(f"No DAM_*.xls files found in: {TEMP_DIR}")

    print(f"Found {len(xls_files)} XLS files in: {TEMP_DIR}")
    print(f"Converted XLSX folder: {CONVERTED_DIR}")

    # 1) Convert all XLS to XLSX fast (single Excel instance)
    converted_xlsx = convert_all_xls_to_xlsx_fast(xls_files)

    # 2) Parse all converted XLSX
    parts = []
    failed = []
    for xlsx_path in tqdm(converted_xlsx, desc="Parsing converted XLSX"):
        try:
            part = parse_one_converted_xlsx(Path(xlsx_path))
            if part.empty:
                failed.append((Path(xlsx_path).name, "PARSED_EMPTY"))
            else:
                parts.append(part)
        except Exception as e:
            failed.append((Path(xlsx_path).name, f"{type(e).__name__}: {e}"))

    if not parts:
        print("❌ No data parsed.")
        if failed:
            print("Examples:")
            for x in failed[:10]:
                print(" ", x)
        return

    # Concatenate all parts
    print(f"\n📊 Concatenating {len(parts)} daily DataFrames...")
    
    # Check data types before concat
    print(f"  First part 'date' dtype: {parts[0]['date'].dtype}")
    print(f"  First part sample dates: {parts[0]['date'].head(2).tolist()}")
    
    df = pd.concat(parts, ignore_index=True)
    
    print(f"  After concat 'date' dtype: {df['date'].dtype}")
    print(f"  After concat sample dates: {df['date'].head(3).tolist()}")
    print(f"  Total rows: {len(df)}")
    
    # CRITICAL FIX: Ensure date column is proper datetime after concat
    # Sometimes concat can mess up the dtype
    if df['date'].dtype == 'object':
        print("  ⚠️ Date column became 'object' - converting back to datetime...")
        df['date'] = pd.to_datetime(df['date'])
        print(f"  ✅ Converted to: {df['date'].dtype}")

    # 3) Correct DST-safe timestamps: build per-day hourly ranges in Europe/Kyiv
    print("\n🕐 Creating timestamps (DST-safe)...")
    
    kyiv = ZoneInfo(KYIV_TZ)
    fixed_cet = ZoneInfo(FIXED_CET_TZ)
    
    df["date"] = pd.to_datetime(df["date"])
    df["hour_seq"] = df["hour_seq"].astype(int)
    
    # We'll assign a timezone-aware timestamp for each row by market day + hour_seq.
    ts_kyiv = np.empty(len(df), dtype="object")
    
    # group by market day (date at midnight)
    for d, idx in df.groupby(df["date"].dt.date).groups.items():
        day_rows = df.loc[idx]
        n = int(day_rows["hour_seq"].max())  # 23/24/25
    
        start = pd.Timestamp(d).tz_localize(kyiv)
    
        # This generates the correct number of hourly instants for that day in Kyiv,
        # including the repeated hour on fall-back day.
        rng = pd.date_range(start=start, periods=n, freq="H")
    
        # hour_seq 1 corresponds to rng[0]
        ts_kyiv[idx] = [rng[h - 1] for h in day_rows["hour_seq"].tolist()]
    
    df["ts_kyiv"] = pd.to_datetime(ts_kyiv)
    df["ts_utc"] = df["ts_kyiv"].dt.tz_convert("UTC")
    df["ts_cet_fixed"] = df["ts_kyiv"].dt.tz_convert(fixed_cet)
    df["ts_cet_fixed_naive"] = df["ts_cet_fixed"].dt.tz_localize(None)
    
    print(f"  ✅ Created {len(df)} timestamps")


    # 4) FX by market date (Kyiv date) - Extract date from datetime column
    # Use the original 'date' column (which is now datetime at midnight Kyiv)
    df["market_date"] = df["date"].dt.date
    
    print(f"\n💱 Exchange rate mapping...")
    print(f"  market_date column type: {type(df['market_date'].iloc[0]) if len(df) > 0 else 'empty'}")
    print(f"  Sample market_date: {df['market_date'].head(3).tolist()}")
    
    # Filter out any invalid dates before creating unique list
    unique_dates = sorted([d for d in df["market_date"].unique() if d is not None and not pd.isna(d)])
    
    print(f"  Unique dates to fetch: {len(unique_dates)}")
    if unique_dates:
        print(f"  Date range: {min(unique_dates)} -> {max(unique_dates)}")
    
    rates = get_eur_rates(unique_dates)
    
    print(f"  Rates fetched: {len(rates)}")
    if rates:
        print(f"  Sample rates: {list(rates.items())[:3]}")
    
    # Use apply with get() to safely map rates
    df["uah_per_eur"] = df["market_date"].apply(lambda x: rates.get(x) if pd.notna(x) else None)
    
    null_count = df['uah_per_eur'].isnull().sum()
    print(f"  uah_per_eur nulls: {null_count}/{len(df)}")
    if null_count == 0:
        print(f"  ✅ All exchange rates mapped successfully!")
        print(f"  Sample uah_per_eur: {df['uah_per_eur'].head(3).tolist()}")
    else:
        print(f"  ⚠️ WARNING: {null_count} rows have no exchange rate!")
    
    df["price_eur_mwh"] = df["price_uah_mwh"] / df["uah_per_eur"]

    # 5) Prepare output files
    df = df.sort_values("ts_cet_fixed_naive").reset_index(drop=True)

    # Determine year range for filenames
    min_date = df["market_date"].min()
    max_date = df["market_date"].max()

    start_year = min_date.year
    end_year = max_date.year
    
    if start_year == end_year:
        year_suffix = f"{start_year}"
    else:
        year_suffix = f"{start_year}_{end_year}"
    
    # Define output filenames
    output_dir = PERIOD_DIR / "dam_prices"
    output_dir.mkdir(parents=True, exist_ok=True)
    OUTPUT_FULL = output_dir / f"UA_OREE_DAM_hourly_prices_{year_suffix}_more_info.xlsx"
    OUTPUT_CLEAN = output_dir / f"UA_OREE_DAM_hourly_prices_{year_suffix}.xlsx"

    cols = [
        "ts_cet_fixed_naive",
        "ts_utc",
        "ts_kyiv",
        "market_date",
        "hour_seq",
        "price_uah_mwh",
        "uah_per_eur",
        "price_eur_mwh",
        "vol_sell_mwh",
        "vol_buy_mwh",
    ]
    cols = [c for c in cols if c in df.columns]

    out = df[cols].copy()

    print("\nParsed rows:", len(out))
    print("Date span:", min_date, "->", max_date)
    print("Max hour_seq observed:", int(out["hour_seq"].max()))

    # 🔧 CRITICAL FIX: Convert timestamps to Excel-compatible format BEFORE saving
    print("\n🔧 Converting timestamps for Excel compatibility...")
    
    if 'ts_cet_fixed_naive' in out.columns:
        out['ts_cet_fixed_naive'] = pd.to_datetime(out['ts_cet_fixed_naive']).dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'ts_utc' in out.columns:
        out['ts_utc'] = out['ts_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'ts_kyiv' in out.columns:
        out['ts_kyiv'] = out['ts_kyiv'].dt.strftime('%Y-%m-%d %H:%M:%S')
    if 'market_date' in out.columns:
        # Convert date objects to string format
        out['market_date'] = pd.to_datetime(out['market_date']).dt.strftime('%Y-%m-%d')
    
    print("✅ Timestamps converted to string format (Excel-compatible)")

    # Save FULL version with all columns
    out.to_excel(OUTPUT_FULL, index=False)
    print(f"\n✅ Saved FULL version: {OUTPUT_FULL}")

    # Create CLEAN version matching AL format
    print("\n📋 Creating clean version (AL format)...")
    
    clean = pd.DataFrame()
    
    # Reconstruct proper datetime objects from strings for the clean version
    clean['date'] = pd.to_datetime(out['market_date'])
    clean['hour'] = out['hour_seq']
    clean['delivery_start_CET'] = pd.to_datetime(out['ts_cet_fixed_naive'])
    clean['price_eur_mwh'] = out['price_eur_mwh']
    clean['market'] = 'OREE Ukraine DAM'
    clean['extracted_at'] = pd.Timestamp.now()
    
    clean.to_excel(OUTPUT_CLEAN, index=False)
    print(f"✅ Saved CLEAN version: {OUTPUT_CLEAN}")
    
    print(f"\n📊 Summary:")
    print(f"  Period: {start_year}" + (f"-{end_year}" if start_year != end_year else ""))
    print(f"  Total hours: {len(clean)}")
    print(f"  Files created:")
    print(f"    1. {OUTPUT_CLEAN.name} (clean AL format)")
    print(f"    2. {OUTPUT_FULL.name} (full details)")

    if failed:
        print(f"\n⚠️ Failed files: {len(failed)} (showing up to 10)")
        for x in failed[:10]:
            print(" ", x)


def main() -> None:
    parser = argparse.ArgumentParser(description="UA OREE DAM downloader + parser")
    parser.add_argument("period", help="Data/output subfolder name, e.g. Jan_2026")
    parser.add_argument(
        "--phase",
        choices=["1", "2", "all"],
        default="all",
        help="Run phase 1 (download), phase 2 (parse), or both (default).",
    )
    args = parser.parse_args()

    set_period_dirs(args.period)

    if args.phase in ("1", "all"):
        run_phase1()

    if args.phase in ("2", "all"):
        print("### PHASE 2 — UA OREE DAM: FAST convert (one Excel) + parse + FX ###")
        print(f"Temp dir: {TEMP_DIR}")
        print("Output: UA_OREE_DAM_hourly_prices_YYYY[_YYYY].xlsx (dynamic naming)")
        print(f"TZ: Kyiv={KYIV_TZ} | fixed CET={FIXED_CET_TZ}")
        run_phase2()


if __name__ == "__main__":
    main()
