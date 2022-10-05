FROM python:3.9.14

MAINTAINER afreecoder@163.com

# 注意，需要排除data/index.db
COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8000

CMD python server.py

