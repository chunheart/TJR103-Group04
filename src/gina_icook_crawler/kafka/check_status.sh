#!/bin/bash
# ===============================================
# check_status.sh
# 功能：檢查 Kafka / Console 狀態與連線情況
# 作者：Mavis 專案維運版本
# ===============================================

echo "Kafka 與 Redpanda Console 狀態檢查中..."
echo "------------------------------------------"

echo "Kafka 服務狀態："
docker logs kafka-server 2>/dev/null | grep "started" | tail -n 2 || echo "⚠️ 尚未啟動或未找到日誌。"

echo ""
echo "Redpanda Console 狀態："
docker logs redpanda-console-server 2>/dev/null | grep "connected" | tail -n 2 || echo "⚠️ 尚未啟動或未找到連線。"

echo ""
echo "容器運行中清單："
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "檢查完畢。"

