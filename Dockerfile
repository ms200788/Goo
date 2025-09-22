# Dockerfile for rendering and running the bot
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy bot
COPY bot.py /app/bot.py

# create data dir
RUN mkdir -p /data

ENV DB_PATH=/data/database.sqlite3
ENV JOB_DB_PATH=/data/jobs.sqlite
ENV PORT=10000

CMD ["python", "bot.py"]