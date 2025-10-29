# ==============================
# 🍳 iCook Crawler - Auto Yesterday v5A (Internal)
# ==============================

# 使用官方 Python 3.11 slim 版
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 加速套件下載（可選用官方或清華鏡像）
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# 安裝固定版本 Poetry（穩定）
RUN pip install poetry==1.8.3

# 複製 pyproject.toml 先行鎖定版本
COPY pyproject.toml ./

# 重新生成 lock 檔，確保一致性
RUN poetry lock --no-interaction

# 複製專案的其餘檔案
COPY . .

# 安裝依賴（不重新安裝專案本體）
RUN poetry install --no-root --no-interaction --no-ansi

# 設定預設啟動命令：每天 09:00 自動抓取「前一天」食譜
CMD ["poetry", "run", "python", "scheduler.py"]
