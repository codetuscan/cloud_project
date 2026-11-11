FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY migrations ./migrations
# Command chosen per service in docker-compose.yml (controller or worker).
CMD ["python", "-c", "print('set a command: app.controller.main or app.worker.main')"]
