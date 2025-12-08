#!/bin/bash
# ===============================================
# restart_kafka.sh
# 功能：一鍵重啟 Kafka + Zookeeper + Redpanda Console
# ===============================================

echo "[1/4] 停止舊容器中..."
docker compose down -v

echo "[2/4] 啟動新容器中..."
docker compose up -d

echo "[3/4] 等待 Kafka 啟動..."
sleep 5

echo "[4/4] 顯示當前容器狀態："
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "Kafka Stack 已重新啟動完成！"
echo "若要開啟 Redpanda Console，請執行： ngrok http 19900"

