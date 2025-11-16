# ========================================
# 🐳 mongo-consumer Dockerfile
# ========================================
FROM python:3.11-slim

WORKDIR /app

# 安裝必要套件
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製程式
COPY consumer_mongo.py .

# 啟動指令
CMD ["python", "consumer_mongo.py"]

