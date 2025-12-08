# -*- coding: utf-8 -*-
"""
iCook Airflow版
-------------------------------------------------------------
    - write_log()：安全落地寫入日誌
    - 關鍵流程自動記錄 log
"""

import re
import os
import json
import time
import argparse
from datetime import datetime, timedelta, date
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pytz

# ==============================
# 台灣時區
# ==============================
tz = pytz.timezone("Asia/Taipei")

# ==============================
# Kafka 初始化
# , "kafka-server:9092"
# ==============================
try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=["kafka-server:29092"],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=100
    )
    TOPIC = "icook_recipes"
    KAFKA_OK = True
    print(" Kafka 連線成功：啟用雙模式（Kafka + CSV）")
except Exception as e:
    producer, TOPIC, KAFKA_OK = None, None, False
    print(f" Kafka 無法連線，改為純 CSV 模式：{e}")

# ==============================
# Airflow 可寫入資料夾
# ==============================
BASE_DIR = "/opt/airflow/logs/icook"
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR  = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")

# ==============================
# Log 寫入
# ==============================
def write_log(msg):
    try:
        ts = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        path = os.path.join(LOG_DIR, f"{date.today()}.log")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        # log 不影響爬蟲
        pass

# ==============================
# 工具
# ==============================
def _text(el):
    return el.get_text(" ", strip=True) if el else ""

def http_get(url, timeout=15):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def get_latest_url(page: int):
    return "https://icook.tw/recipes/latest" if page == 1 else f"https://icook.tw/recipes/latest?page={page}"

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_page": 0, "last_date": None}

def save_progress(date_str, page):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_date": date_str, "last_page": page}, f, ensure_ascii=False, indent=2)

def backup_progress():
    if os.path.exists(PROGRESS_FILE):
        backup_path = PROGRESS_FILE.replace(".json", "_backup.json")
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.rename(PROGRESS_FILE, backup_path)
        print(f"[強制模式] 已忽略進度並覆蓋備份 → {os.path.basename(backup_path)}")

def daterange_inclusive(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

# ==============================
# 拆解重量
# ==============================
def split_qty(qty_raw):
    if not qty_raw:
        return "", ""
    qty_raw = qty_raw.strip()
    if re.match(r"^[\u4e00-\u9fa5]+$", qty_raw):
        return "", qty_raw

    replacements = {"½": "1/2", "¼": "1/4", "¾": "3/4", "⅓": "1/3", "⅔": "2/3"}
    for k, v in replacements.items():
        qty_raw = qty_raw.replace(k, v)

    match = re.match(r"^([0-9]+(?:/[0-9]+)?(?:\.[0-9]+)?)\s*([a-zA-Z\u4e00-\u9fa5]*)$", qty_raw)
    if not match:
        return "", qty_raw

    num_str, unit = match.group(1), match.group(2).strip() or ""
    try:
        if "/" in num_str:
            a, b = num_str.split("/")
            num = round(float(a) / float(b), 3)
        else:
            num = float(num_str)
    except Exception:
        num = num_str
    return f"{num}", unit

# ==============================
# 解析食譜
# ==============================
def parse_recipe_info(html, url, debug=False):
    soup = BeautifulSoup(html, "lxml")
    rid = re.search(r"/recipes/(\d+)", url).group(1)
    title = _text(soup.select_one("h1#recipe-name.title") or soup.select_one("h1.title") or soup.select_one("h1"))
    author = _text(soup.select_one("a.author-name-link"))
    time_tag = soup.select_one("time[datetime]")
    pub = time_tag["datetime"][:10] if time_tag and time_tag.get("datetime") else ""

    servings = f"{_text(soup.select_one('div.servings-info span.num'))} {_text(soup.select_one('div.servings-info span.unit'))}".strip()
    cook_time = f"{_text(soup.select_one('div.time-info span.num'))} {_text(soup.select_one('div.time-info span.unit'))}".strip()

    if debug:
        print(f"    [info] {rid} | {pub or 'N/A'} | {title[:40]}")

    ingredients = []
    for group in soup.select("div.ingredients-groups div.group"):
        group_type = _text(group.select_one(".group-name")) or "食材"
        for li in group.select("li.ingredient"):
            name = _text(li.select_one(".ingredient-name a.ingredient-search"))
            qty_raw = _text(li.select_one(".ingredient-unit"))
            if name:
                w_val, w_unit = split_qty(qty_raw)
                ingredients.append((group_type, name, qty_raw, w_val, w_unit))
    return rid, title, author, pub, ingredients, servings, cook_time

# ==============================
# 主爬蟲流程
# ==============================
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Referer": "https://icook.tw",
}

BASE = "https://icook.tw"

def crawl_latest_with_kafka(since, before, start_page=1, sleep=1.5, max_pages=20, debug=False, force=False, excel_safe=False):

    write_log(f"啟動爬蟲 since={since} before={before}")
    print(f"爬蟲時間（台灣時區）: {datetime.now(tz).isoformat(timespec='seconds')}")

    since_d = datetime.strptime(since, "%Y-%m-%d").date()
    before_d = datetime.strptime(before, "%Y-%m-%d").date()

    collected = {d.isoformat(): [] for d in daterange_inclusive(since_d, before_d)}


    if force:
        backup_progress()
    else:
        progress = load_progress()
        if progress["last_date"] == since:
            start_page = progress.get("last_page", 0) + 1
            write_log(f"🔁 從進度接續第 {start_page} 頁")
            print(f"🔁 從進度接續第 {start_page} 頁")

    stop_flag = False

    for page in range(start_page, start_page + max_pages):
        if stop_flag:
            break

        url_page = get_latest_url(page)
        write_log(f"抓取列表頁 page={page} {url_page}")

        if debug:
            print(f"\n[DEBUG] 抓取列表頁: {url_page}")

        try:
            html = http_get(url_page)
        except Exception as e:
            write_log(f"列表頁讀取錯誤: {e}")
            continue

        soup = BeautifulSoup(html, "lxml")
        links = [urljoin(BASE, a["href"]) for a in soup.select('a[href^="/recipes/"]') if re.match(r"^/recipes/\d+$", a["href"])]

        for link in links:
            try:
                write_log(f"抓取食譜頁 {link}")
                rid, title, author, pub, ings, servings, cook_time = parse_recipe_info(http_get(link), link, debug)

                if not pub:
                    continue

                pub_date = datetime.strptime(pub, "%Y-%m-%d").date()

                if pub_date > before_d:
                    continue
                if pub_date < since_d:
                    write_log(f"提前結束：{pub_date} < {since}")
                    print(f"偵測到 {pub_date} 早於 {since}，停止.")
                    stop_flag = True
                    break

                rows = []
                for gtype, ing, qty_raw, w_val, w_unit in ings or [("", "", "", "", "")]:
                    record = {
                        "ID": rid,
                        "食譜名稱": title,
                        "作者": author,
                        "來源": link,
                        "食用人數": servings,
                        "料理時間": cook_time,
                        "類型": gtype,
                        "名稱": ing,
                        "原始重量": qty_raw,
                        "重量": w_val,
                        "重量單位": w_unit,
                        "上線日期": pub,
                        "爬蟲時間": datetime.now(tz).isoformat(timespec="seconds"),
                        "是否有食材": int(bool(ings)),
                        "site": "icook"
                    }
                    rows.append(record)

                    if KAFKA_OK:
                        producer.send(TOPIC, record)

                collected[pub].extend(rows)
                save_progress(since, page)
                time.sleep(sleep)

            except Exception as e:
                write_log(f"食譜頁錯誤: {link} | {e}")
                if debug:
                    print(f"[error] {link} {e}")
                continue

        if stop_flag:
            break

        time.sleep(sleep)

    # =========================
    # CSV 輸出
    # =========================
    col_order = ["ID", "食譜名稱", "作者", "來源", "食用人數",
                 "料理時間", "類型", "名稱", "原始重量", "重量",
                 "重量單位", "上線日期", "爬蟲時間",
                 "是否有食材", "site"]

    for d, rows in sorted(collected.items()):
        if not rows:
            continue
        outpath = os.path.join(DATA_DIR, f"{d}.csv")
        df = pd.DataFrame(rows, dtype=str)[col_order]

        if excel_safe:
            df = df.map(lambda x: f"'{x}" if isinstance(x, str) and re.match(r"^[0-9.]+$", x) else x)

        df.to_csv(outpath, index=False, encoding="utf-8-sig", quoting=1)
        write_log(f"已輸出 CSV: {outpath} rows={len(rows)}")
        print(f"[saved] {d} -> {outpath} (rows={len(rows)})")

    if KAFKA_OK:
        producer.flush()
        producer.close()

    write_log("爬蟲完成")
    print("完成。")


# ==============================
# CLI 主程式
# ==============================
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    ap.add_argument("--before", required=True)
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--sleep", type=float, default=1.5)
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--excel-safe", action="store_true")
    args = ap.parse_args()

    crawl_latest_with_kafka(
        args.since,
        args.before,
        args.start_page,
        args.sleep,
        args.max_pages,
        args.debug,
        args.force,
        args.excel_safe
    )

