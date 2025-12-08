# iCook Crawler - Docker 自動排程版

本版本會每天 **09:00** 自動抓取「前一天」的 iCook 食譜資料，  
支援容器重啟自動續爬，並將結果存放於 `/app/data/`。

---

## 一、建立與執行容器

```powershell
# 進入專案資料夾
cd "C:\Users\<你的名字>\icook_crawler_docker_auto_resume_v6"

# 建立映像檔
docker build -t icook-crawler-auto .

# 啟動容器（背景執行）
docker run -d --name icook_auto icook-crawler-auto

# 查看執行日誌
docker logs -f icook_auto

二、資料儲存位置
路徑	說明
/app/data/2025-10-30.csv	當日爬取結果
/app/data/progress.json	儲存上次爬取進度（容器重啟會自動續爬）


三、自動排程說明

每天早上 09:00 自動執行 daily.py。

爬取「前一天」的食譜。

每天完成後自動產生 CSV 檔。

若爬蟲中斷，容器重啟後會從 progress.json 接續。

四、常用指令
# 查看容器狀態
docker ps

# 查看日誌（即時監控）
docker logs -f icook_auto

# 手動執行爬蟲（可測試）
docker exec -it icook_auto poetry run python -m icook_crawler.daily `
  --since 2025-10-29 --before 2025-10-30 --start-page 1 --max-pages 10 --sleep 1.5

# 停止容器
docker stop icook_auto

# 重新啟動
docker start icook_auto

# 刪除容器
docker rm -f icook_auto

五、清除資料（如要重新開始）
docker rm -f icook_auto
docker rmi icook-crawler-auto
Remove-Item -Recurse -Force .\data\

六、說明

scheduler.py：控制每天 09:00 執行爬蟲。

daily.py：爬取邏輯（支援 --start-page、--max-pages、自動續爬）。

progress.json：自動記錄進度，如：

{ "last_date": "2025-10-30", "last_page": 4 }


容器重啟後會自動從 last_page + 1 繼續。
