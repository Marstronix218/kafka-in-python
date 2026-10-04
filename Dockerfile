FROM python:3.12-slim
WORKDIR /app
COPY eventlog /app/eventlog
COPY config /app/config
CMD ["python", "-m", "eventlog", "--help"]
