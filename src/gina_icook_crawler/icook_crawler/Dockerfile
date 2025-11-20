# ===========================
# 🍳  iCook Crawler Dockerfile (Enhanced)
# ===========================
FROM python:3.11-slim

WORKDIR /app

# 安裝系統依賴（確保 bs4/kafka-python 不出現 build 錯誤）
RUN apt-get update && apt-get install -y \
    gcc g++ build-essential libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Poetry（官方推薦方式）
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# 複製專案設定與程式碼
COPY . .

# 安裝所有依賴（包含 bs4 與 kafka-python）
RUN poetry install --no-root && \
    poetry add beautifulsoup4 kafka-python

# 建立資料夾 (用於保存 CSV / progress.json)
RUN mkdir -p /app/data /app/logs

# 預設執行 scheduler
CMD ["poetry", "run", "python", "scheduler.py"]

