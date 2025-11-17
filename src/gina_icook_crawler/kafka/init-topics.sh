#!/bin/bash
echo "📡 Checking Kafka topics..."
kafka-topics --bootstrap-server kafka-server:29092 --list

echo "📦 Creating topic icook_recipes..."
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka-server:29092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic icook_recipes

echo "✅ Topic icook_recipes created or already exists."
