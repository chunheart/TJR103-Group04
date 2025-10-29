# TJR103-Group04
第四組專題


## 🐳 一、建立與執行容器(環境設定)

```powershell
# 進入專案資料夾

cd "C:\Users\<你的名字>\TJR103-Group04"

# 建立映像檔
docker build -t icook-crawler-internal .

# 啟動容器（背景執行）
docker run -d --rm \
    --name recipe_coemission \
    -v "$PWD":/app \
    icook-crawler-internal

# 查看日誌
docker logs -f recipe_coemission
git 
```

## 二、Features
### 🍳 iCook Crawler - Docker 自動排程版 (v5A Internal)
本版本會每天 09:00 自動抓取「前一天」的 iCook 食譜資料，並將結果存放於容器內 `/app/data/`。
