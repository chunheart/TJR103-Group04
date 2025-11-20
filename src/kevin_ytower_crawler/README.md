# Ytower Crawler (楊桃美食網爬蟲)
此模組負責抓取 [楊桃美食網](https://www.ytower.com.tw) 的食譜資料，並輸出清洗後的 CSV。

##  執行指令
請回到**專案根目錄** ( `TJR103-Group04...` )，執行以下指令：

##  如何執行
請在專案根目錄下執行以下指令：

Bash
poetry run python3 src/kevin_ytower_crawler/main.py
執行後，爬蟲會自動執行以下動作：


## 進入 Scrapy 專案環境。
## 開始爬取設定好的分類頁面。
將結果輸出至 ytower_csv_output/ 資料夾。

## 輸出資料格式
執行完成後會產生 ytower_all_recipes.csv，包含以下 12 個欄位（依序）：

欄位名稱	說明	範例值
ID	食譜編號	F04-0579
Recipe_Title	食譜標題	金針菇炒絲瓜
Author	作者 (預設為 null)	null
Recipe_URL	原始連結	https://www.ytower...
Servings	食用人數 (預設為 null)	null
Type	食材分類	食材 或 調味料
Ingredient_Name	食材名稱	金針菇
Weight	重量數值	1
Unit	單位	包
Publish_Date	發布日期	2012-06-22
Crawl_Time	爬取時間	2025-11-17 14:30:00
site	來源站點	ytower


```bash
poetry run python3 src/kevin_ytower_crawler/main.py
