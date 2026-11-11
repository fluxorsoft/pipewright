# Pipewright capture receiver (Pipewright 1.4.3, FR-98): KServe's logger posts each request and answer here; one JSON
# line per sampled record goes to stdout, and a log sink copies them into the tenant's dataset. Standard library only,
# so the image is the Python base and one file. Build context: the repository root:
#   docker build -f images/capture-receiver.Dockerfile -t <repo>/pipewright-capture-receiver:<version> .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

COPY src/pipewright/serving/receiver.py /app/receiver.py
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["python", "/app/receiver.py"]
