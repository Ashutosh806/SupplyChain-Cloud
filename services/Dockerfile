# One Dockerfile for all Python microservices; pick the service with
#   docker build --build-arg SERVICE=booking-service -t booking-service services/
FROM python:3.12-slim

ARG SERVICE
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/gen \
    SERVICE_NAME=${SERVICE}

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# gRPC stubs are generated at build time from the shared .proto contracts
COPY proto/ /app/proto/
RUN mkdir -p /app/gen && python -m grpc_tools.protoc -I /app/proto \
      --python_out=/app/gen --grpc_python_out=/app/gen /app/proto/*.proto

COPY common/sc_common /app/sc_common
COPY ${SERVICE}/app /app/app

RUN useradd --uid 10001 --create-home app && mkdir -p /data && chown -R app /app /data
USER 10001
EXPOSE 8080 50051 50052
CMD ["python", "-m", "app.main"]
