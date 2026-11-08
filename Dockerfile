FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app
COPY . /app

RUN useradd --system --uid 10001 --create-home runner \
    && chmod 0755 /app/start.sh \
    && mkdir -p /data \
    && chown -R runner:runner /app /data

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/app/start.sh"]
