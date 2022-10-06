FROM python:3.9.14

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

RUN mkdir data \
    && pip install -r requirements.txt

EXPOSE 8000

CMD python server.py

