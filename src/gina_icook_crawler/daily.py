# -*- coding: utf-8 -*-
"""
iCook 自動爬蟲 daily.py (2025 Latest-only + start-page 版本)
-------------------------------------------------------------
✅ 僅使用 /recipes/latest?page=N
✅ 新增 --start-page 參數：可指定從任意頁開始爬
✅ 自動停止：若日期早於 --since 即結束
✅ 每日輸出 CSV：data/YYYY-MM-DD.csv
"""

import re
import time
import argparse
import os
from datetime import datetime, timedelta, timezone, date
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup

# --------------------------------------------------
BASE = "https://icook.tw"
LATEST_URL = "https://icook.tw/recipes/latest?page={page}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def http_get(url, timeout=15):
    """安全 HTTP GET"""
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def _text(el):
    """安全文字擷取"""
    return el.get_text(" ", strip=True) if el else ""


def daterange_inclusive(start: date, end: date):
    """日期範圍產生器"""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# --------------------------------------------------
def parse_list_latest(html: str, debug=False):
    """解析 /recipes/latest?page=N 頁面，取出食譜連結"""
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select('a[href^="/recipes/"]'):
        href = a.get("href", "").strip()
        # 確保是食譜連結而非 tag
        if re.match(r"^/recipes/\d+$", href):
            full = urljoin(BASE, href)
            if full not in links:
                links.append(full)
                if debug:
                    print(f"  ↳ {full}")
    return links


def parse_recipe_info(html: str, url: str, debug=False):
    """解析單篇食譜內頁（含發表日期與食材）"""
    soup = BeautifulSoup(html, "lxml")
    rid = re.search(r"/recipes/(\d+)", url).group(1)
    title_el = soup.select_one(
        "h1#recipe-name.title") or soup.select_one("h1.title") or soup.select_one("h1")
    title = _text(title_el)
    author = _text(soup.select_one("a.author-name-link"))

    # 發表日期
    time_tag = soup.select_one("time[datetime]")
    if time_tag and time_tag.get("datetime"):
        pub = time_tag["datetime"][:10]
    else:
        pub = ""
    if debug:
        print(f"    [info] {rid} | {pub or 'N/A'} | {title[:40]}")

    ingredients = []
    for li in soup.select("div.recipe-details-ingredients div.ingredients-groups li.ingredient"):
        name = _text(li.select_one(".ingredient-name a.ingredient-search"))
        qty = _text(li.select_one(".ingredient-unit"))
        if name:
            ingredients.append((name, qty))

    return rid, title, author, pub, ingredients


# --------------------------------------------------
def crawl_latest_only(since: str, before: str, start_page: int, outdir="data", sleep=1.5, max_pages=20, debug=False):
    """僅用 Latest 模式爬取，支援自訂起始頁"""
    os.makedirs(outdir, exist_ok=True)
    since_d = datetime.strptime(since, "%Y-%m-%d").date()
    before_d = datetime.strptime(before, "%Y-%m-%d").date()
    collected = {d.isoformat(): []
                 for d in daterange_inclusive(since_d, before_d)}

    print(f"🕓 使用 iCook /recipes/latest 模式擷取食譜（從第 {start_page} 頁開始）")

    stop_early = False
    oldest_date = None

    for page in range(start_page, start_page + max_pages):
        if stop_early:
            break

        page_url = LATEST_URL.format(page=page)
        try:
            html = http_get(page_url)
        except Exception as e:
            print("[error] 無法讀取頁面:", e)
            continue

        if debug:
            print(f"\n=== 第 {page} 頁 ===")
            print(f"來源: {page_url}")

        links = parse_list_latest(html, debug)
        if not links:
            print(f"[page {page}] 無更多食譜，停止。")
            break

        for url in links:
            try:
                rid, title, author, pub, ings = parse_recipe_info(
                    http_get(url), url, debug)
                if not pub:
                    continue

                try:
                    pub_date = datetime.strptime(pub, "%Y-%m-%d").date()
                except Exception:
                    continue

                # 若超過 before_d 則略過（太新）
                if pub_date > before_d:
                    continue

                # 若早於 since_d 則自動停止
                if pub_date < since_d:
                    stop_early = True
                    print(f"⏹️  已爬到最舊日期 {pub_date} 早於 {since_d}，自動結束。")
                    break

                rows = []
                if ings:
                    for ing, qty in ings:
                        rows.append(dict(
                            recipe_id=rid,
                            url=url,
                            title=title,
                            author=author,
                            published_date=pub,
                            ingredient=ing,
                            qty_raw=qty,
                            has_ingredient=1,
                            fetched_at=datetime.now(
                                timezone.utc).isoformat(timespec="seconds")
                        ))
                else:
                    rows.append(dict(
                        recipe_id=rid,
                        url=url,
                        title=title,
                        author=author,
                        published_date=pub,
                        ingredient="",
                        qty_raw="",
                        has_ingredient=0,
                        fetched_at=datetime.now(
                            timezone.utc).isoformat(timespec="seconds")
                    ))

                collected[pub].extend(rows)
                oldest_date = pub_date if not oldest_date or pub_date < oldest_date else oldest_date
                if debug:
                    print(
                        f"      [ok] {pub} {title[:25]} ({len(ings)} ingredients)")

                time.sleep(sleep)

            except Exception as e:
                if debug:
                    print("[error]", url, e)
                continue

        time.sleep(sleep)

    # 若仍未達 since_d 提示使用者
    if not stop_early and oldest_date and oldest_date > since_d:
        print(f"⚠️ 尚未找到早於 {since_d} 的食譜，請提高 --max-pages 或調整 --start-page 再試。")

    # 依日期輸出 CSV
    for d, rows in sorted(collected.items()):
        if not rows:
            continue
        outpath = f"{outdir}/{d}.csv"
        df = pd.DataFrame(rows)
        df.to_csv(outpath, index=False, encoding="utf-8-sig")
        print(f"[saved] {d} -> {outpath} (rows={len(df)})")


# --------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="iCook /recipes/latest 模式爬蟲（支援起始頁）")
    ap.add_argument("--since", required=True, help="起始日期 (YYYY-MM-DD)")
    ap.add_argument("--before", required=True, help="結束日期 (YYYY-MM-DD)")
    ap.add_argument("--start-page", type=int, default=1, help="起始頁碼（預設第1頁）")
    ap.add_argument("--sleep", type=float, default=1.5, help="每筆間隔秒數")
    ap.add_argument("--max-pages", type=int,
                    default=20, help="最大頁數（自動停止可提前結束）")
    ap.add_argument("--outdir", default="data", help="輸出資料夾")
    ap.add_argument("--debug", action="store_true", help="啟用詳細除錯輸出")
    args = ap.parse_args()

    crawl_latest_only(args.since, args.before, args.start_page,
                      args.outdir, args.sleep, args.max_pages, args.debug)


if __name__ == "__main__":
    main()
