# 🍳 iCook Crawler - Docker 自動排程版 (v5A Internal)

本版本會每天 09:00 自動抓取「前一天」的 iCook 食譜資料，並將結果存放於容器內 `/app/data/`。

---

## 🐳 一、建立與執行容器

```powershell
# 進入專案資料夾
cd "C:\Users\<你的名字>\icook_crawler_docker_auto_yesterday_v5A_internal"

# 建立映像檔
docker build -t icook-crawler-internal .

# 啟動容器（背景執行）
docker run -d --name icook_auto_in icook-crawler-internal

# 查看日誌
docker logs -f icook_auto_in
