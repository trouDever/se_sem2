# Один образ на все сервисы: модуль выбирается переменной SERVICE (catalog|inventory|order|payment|notification|analytics)
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONUNBUFFERED=1 SERVICE=catalog
USER 1000
EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn ${SERVICE}_service:app --host 0.0.0.0 --port 8080 --no-access-log --workers ${WORKERS:-1}"]
