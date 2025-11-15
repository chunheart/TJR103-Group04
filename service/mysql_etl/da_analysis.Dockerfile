FROM python:3.12-slim-bullseye

# 工作目錄與對外設定
WORKDIR /work
EXPOSE 8888
ENV JUPYTER_TOKEN=mytoken123

# 系統依賴（視你的套件需求調整）
RUN apt-get update && apt-get install -y --no-install-recommends build-essential vim curl git \
  && rm -rf /var/lib/apt/lists/*

# 安裝 jupyter 與你要的科學套件
# COPY requirements.txt /tmp/requirements.txt (if exists)
RUN pip install --upgrade pip
RUN pip install --no-cache-dir jupyterlab notebook numpy pandas matplotlib seaborn \
  requests pymysql pymongo cryptography

# 以前景方式啟動（容器才不會退出）
CMD ["jupyter","lab","--ip=0.0.0.0","--port=8888","--no-browser","--allow-root","--NotebookApp.token=mytoken123"]
