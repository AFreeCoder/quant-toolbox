FROM python:3.9.16-slim

MAINTAINER afreecoder@163.com

COPY conf /app/conf
COPY cron /app/cron
COPY fund_company /app/fund_company
COPY qtools /app/qtools
COPY source /app/source
COPY util /app/util
COPY server.py /app
COPY requirements.txt /app

WORKDIR /app

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' > /etc/timezone

RUN mkdir data logs\
    && pip install -r requirements.txt

EXPOSE 8000

CMD ["python", "server.py", "release"]

