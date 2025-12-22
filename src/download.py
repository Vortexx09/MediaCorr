import argparse
import concurrent.futures as cf
import datetime as dt
import gzip
import io
import os
import random
import re
import sys
import time
from typing import Iterable, List, Tuple

import requests
from tqdm import tqdm

BASE = "https://data.commoncrawl.org/crawl-data/CC-NEWS/{year}/{month}/"
PATHS_NAME = "warc.paths.gz"

# Example filename: CC-NEWS-20240101002957-01499.warc.gz
FILENAME_RE = re.compile(r"CC-NEWS-(\d{14})-\d+\.warc\.gz$")


def month_iter(start_date: dt.date, end_date: dt.date) -> Iterable[Tuple[int, int]]:
    """Yield (year, month) tuples covering the closed interval [start_date, end_date]."""
    y, m = start_date.year, start_date.month
    while (y < end_date.year) or (y == end_date.year and m <= end_date.month):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def parse_timestamp_from_key(key: str) -> dt.datetime | None:
    """
    Extract UTC timestamp from a CC-NEWS filename (YYYYMMDDHHMMSS).
    Key can be a full S3 key like 'crawl-data/CC-NEWS/2024/01/CC-NEWS-...warc.gz'.
    """
    fname = os.path.basename(key)
    m = FILENAME_RE.search(fname)
    if not m:
        return None
    ts = m.group(1)
    try:
        # CC-NEWS timestamps are UTC (crawl time)
        return dt.datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def fetch_paths_for_month(year: int, month: int, timeout: int = 60) -> List[str]:
    """
    Download warc.paths.gz for a given (year, month) and return full HTTPS URLs.
    Returns [] if the file is missing (404).
    """
    base = BASE.format(year=year, month=f"{month:02d}")
    url = f"{base}{PATHS_NAME}"
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 404:
            # Some months may not have a published paths list
            return []
        r.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(r.content)) as gz:
            lines = [ln.decode("utf-8").strip() for ln in gz.readlines()]
        # Each line is a bucket key like: crawl-data/CC-NEWS/YYYY/MM/CC-NEWS-...warc.gz
        return [f"https://data.commoncrawl.org/{key}" for key in lines if key.endswith(".warc.gz")]
    except requests.RequestException as e:
        print(f"[warn] Failed to fetch paths for {year}-{month:02d}: {e}")
        return []


def filter_urls_by_date(urls: List[str], start_dt: dt.datetime, end_dt: dt.datetime) -> List[str]:
    """Keep only URLs whose filename timestamp is within [start_dt, end_dt]."""
    kept = []
    for u in urls:
        ts = parse_timestamp_from_key(u)
        if ts and start_dt <= ts <= end_dt:
            kept.append(u)
    return kept


def select_urls(urls: List[str], limit: int, strategy: str) -> List[str]:
    """Select up to limit URLs by strategy: oldest | newest | random."""
    if not urls:
        return []
    # Sort by timestamp for deterministic selection
    urls_with_ts = [(u, parse_timestamp_from_key(u)) for u in urls]
    urls_with_ts = [pair for pair in urls_with_ts if pair[1] is not None]
    if strategy == "oldest":
        urls_with_ts.sort(key=lambda x: x[1])
    elif strategy == "newest":
        urls_with_ts.sort(key=lambda x: x[1], reverse=True)
    elif strategy == "random":
        random.shuffle(urls_with_ts)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")
    return [u for u, _ in urls_with_ts[:limit]]


def download_one(url: str, dest_dir: str, retries: int = 3, backoff: float = 1.5) -> Tuple[str, bool, str]:
    """
    Download a single URL to dest_dir with simple retry/backoff.
    Returns (filename, success, message).
    """
    os.makedirs(dest_dir, exist_ok=True)
    fname = os.path.basename(url)
    out_path = os.path.join(dest_dir, fname)
    if os.path.exists(out_path):
        return fname, True, "exists"

    attempt = 0
    last_err = ""
    while attempt <= retries:
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(out_path, "wb") as f, tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False,
                    desc=fname,
                ) as bar:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
            return fname, True, "ok"
        except requests.RequestException as e:
            last_err = str(e)
            attempt += 1
            if attempt <= retries:
                sleep_s = backoff ** attempt
                time.sleep(sleep_s)
            else:
                break
    return fname, False, last_err


def main():
    ap = argparse.ArgumentParser(description="Download CC-NEWS WARC files by date range (HTTPS only).")
    ap.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    ap.add_argument("--limit", type=int, default=3, help="Max number of WARC files to download in the range")
    ap.add_argument("--strategy", choices=["oldest", "newest", "random"], default="oldest",
                    help="Selection strategy when more than --limit files match")
    ap.add_argument("--concurrency", type=int, default=3, help="Parallel downloads (threads)")
    ap.add_argument("--out", default="data/raw", help="Output directory")
    ap.add_argument("--dry-run", action="store_true", help="List selected URLs but do not download")
    args = ap.parse_args()

    # Parse dates → UTC datetimes covering full days
    try:
        start_date = dt.date.fromisoformat(args.start)
        end_date = dt.date.fromisoformat(args.end)
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD.")
        sys.exit(1)
    if end_date < start_date:
        print("End date must be on/after start date.")
        sys.exit(1)

    start_dt = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.timezone.utc)
    end_dt = dt.datetime.combine(end_date, dt.time.max, tzinfo=dt.timezone.utc)

    # 1) Gather monthly lists
    all_urls: List[str] = []
    for y, m in month_iter(start_date, end_date):
        urls = fetch_paths_for_month(y, m)
        if urls:
            all_urls.extend(urls)
        else:
            print(f"[info] No paths list for {y}-{m:02d} (skipped).")

    if not all_urls:
        print("No URLs discovered in the given range/months.")
        sys.exit(0)

    # 2) Filter by filename timestamp
    in_range = filter_urls_by_date(all_urls, start_dt, end_dt)
    if not in_range:
        print("No WARC files match the specified date range.")
        sys.exit(0)

    # 3) Select up to --limit according to strategy
    selected = select_urls(in_range, args.limit, args.strategy)
    print(f"Selected {len(selected)} / {len(in_range)} matching URLs (strategy={args.strategy}, limit={args.limit}).")

    for u in selected:
        print(" -", u)

    if args.dry_run:
        return

    # 4) Download concurrently
    results = []
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(download_one, u, args.out) for u in selected]
        for fut in cf.as_completed(futs):
            results.append(fut.result())

    # 5) Summary
    ok = sum(1 for _, s, _ in results if s)
    fail = len(results) - ok
    print(f"Downloads complete: success={ok}, failed={fail}")
    for fname, success, msg in results:
        if not success:
            print(f"  [fail] {fname}: {msg}")


if __name__ == "__main__":
    main()